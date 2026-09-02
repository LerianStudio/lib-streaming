//go:build unit

// Package boundary_test guards the module boundary that the obs package
// exists to create.
//
// lib-streaming must not name a github.com/LerianStudio/lib-observability
// type anywhere a consumer can reach. Go types have nominal identity, so a
// single exported field or parameter of a lib-observability type re-pins every
// consumer of lib-streaming to whichever lib-observability major lib-streaming
// chose - which is how a Fiber upgrade inside lib-observability/middleware
// ended up blocking midaz. This test walks every non-test .go file in the
// module with go/ast and fails if any exported symbol mentions such a type.
//
// Unlike the equivalent guard in lib-commons, this one DOES scan internal/.
// lib-streaming's root package is a thin alias layer - `type EmitterOption =
// producer.EmitterOption`, `type TransportAdapterOptions =
// producer.TransportAdapterOptions` - so an exported field on an internal
// struct is reachable by a consumer under a root-package name, and skipping
// internal/ would leave the biggest hole uncovered. Calling lib-observability
// (tracing, assert, runtime, redaction, constants, log.NewNop) is untouched by
// this test: only what appears in an exported signature counts.
//
// A raw selector scan is not enough, because Go lets a package launder the
// identity through its own names:
//
//	type internalLogger = log.Logger              // declared in file A
//	type Config struct{ Logger internalLogger }   // used in file B
//
// `Config.Logger` still IS `log.Logger` - alias declarations create no new
// type - so the consumer is still pinned, yet no lib-observability selector
// appears anywhere near `Config`, and file B need not import the module at
// all. The same laundering works through an unexported defined type that
// carries a lib-observability type on its own exported surface. So the guard
// runs in two passes: pass one indexes every package-level type declaration in
// the module and computes, to a fixpoint, which names leak; pass two reports
// exported symbols that reference a lib-observability selector OR one of those
// names. Import scope is per file and package scope is not, so each
// declaration is resolved against the import table of the file that wrote it.
//
// The indexing pass deliberately covers directories the reporting pass skips
// (billing/, allowlisted packages): a name declared there must still be
// resolvable when a scanned package aliases it.
//
// This test uses go/ast and not go/types on purpose. See TestGuard_StaysASTOnly.
//
// This test is the thing that prevents the regression. Do not weaken it. The
// allowlist below is empty, by design.
package boundary_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// libObservability is the module whose types must not reach the public API.
const libObservability = "github.com/LerianStudio/lib-observability"

// allowedPackages are the packages permitted to name a lib-observability type
// on their exported surface, keyed by module-relative directory.
//
// It is empty, and that is the point. Since lib-observability v4 every
// parameter position of that library is declared with universal types, so a
// value obtained from it can be handed straight back as an obs contract with
// nothing named and no adapter written. No package of lib-streaming has an
// excuse. Adding an entry re-opens the coupling obs exists to close.
var allowedPackages = map[string]bool{}

// violation is one exported symbol that mentions a lib-observability type.
type violation struct {
	file   string
	line   int
	symbol string
	typ    string
}

func (v violation) String() string {
	return fmt.Sprintf("%s:%d: exported %s references %s", v.file, v.line, v.symbol, v.typ)
}

// typeDecl is one package-level type declaration together with the import
// table of the file that declares it. Package scope spans every file of a
// package but import scope does not, so a name can only be resolved against
// the imports of its own file.
type typeDecl struct {
	spec    *ast.TypeSpec
	imports map[string]string
}

// index is the module-wide view the guard resolves names against.
type index struct {
	// modulePath is the module path from go.mod, used to tell an
	// intra-module import from a third-party one.
	modulePath string
	// pkgDir maps an import path of this module to its module-relative
	// directory.
	pkgDir map[string]string
	// pkgName maps an import path of this module to the package name
	// actually declared there. Go only conventionally ties that name to the
	// last path segment, so an unaliased import of such a package is
	// referenced by a selector the path cannot predict.
	pkgName map[string]string
	// decls holds every package-level type declaration, keyed by
	// module-relative directory then by type name.
	decls map[string]map[string]typeDecl
	// leaks names, per directory, the package-level types a consumer can
	// reach a lib-observability type through, mapped to the type it leaks.
	leaks map[string]map[string]string
}

func TestPublicAPI_DoesNotMentionLibObservability(t *testing.T) {
	t.Parallel()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	idx, err := newIndex(root)
	require.NoError(t, err)

	var violations []violation

	err = walkGoFiles(root, func(path, rel string) error {
		dir := filepath.ToSlash(filepath.Dir(rel))

		// Generated protobuf stubs and vendored trees are not
		// hand-written API surface. They are indexed, not reported.
		if dir == "billing" || strings.HasPrefix(dir, "billing/") || allowedPackages[dir] {
			return nil
		}

		found, scanErr := idx.scanFile(path, filepath.ToSlash(rel), dir)
		if scanErr != nil {
			return scanErr
		}

		violations = append(violations, found...)

		return nil
	})
	require.NoError(t, err)

	if !assert.Empty(t, violations, "lib-observability types must not appear on the public API of lib-streaming") {
		for _, v := range violations {
			t.Log(v.String())
		}
	}
}

// TestGuard_StaysASTOnly pins the guard to go/ast and keeps it out of
// go/types, so the next reader does not "upgrade" it into something that stops
// working exactly when it is needed.
//
// go/types would resolve aliases for free, but loading a package graph means
// golang.org/x/tools/go/packages, which lands in the go.sum of every consumer
// of lib-streaming - the coupling this whole boundary exists to avoid - and a
// type-checked scan only reports anything when the module already compiles.
// The moment this guard earns its keep is a breaking dependency bump, when
// half the module does not compile; an AST scan still reads every file and
// still answers. The cost is that identity laundering has to be modelled by
// hand, which is what the index above does. Pay that cost.
func TestGuard_StaysASTOnly(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "boundary_test.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)

	for _, imp := range file.Imports {
		path, unquoteErr := strconv.Unquote(imp.Path.Value)
		require.NoError(t, unquoteErr)

		assert.NotEqual(t, "go/types", path,
			"the guard must stay AST-only; see this test's doc comment")
		assert.NotEqual(t, "go/importer", path,
			"the guard must stay AST-only; see this test's doc comment")
		assert.NotContains(t, path, "golang.org/x/tools",
			"the guard must not put golang.org/x/tools in a consumer go.sum")
	}
}

// walkGoFiles calls visit for every non-test .go file under root, with its
// absolute path and its module-relative path.
func walkGoFiles(root string, visit func(path, rel string) error) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			if entry.Name() == ".git" {
				return fs.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}

		return visit(path, filepath.ToSlash(rel))
	})
}

var modulePathPattern = regexp.MustCompile(`(?m)^module\s+(\S+)\s*$`)

// newIndex parses every non-test .go file in the module, records its
// package-level type declarations, and resolves to a fixpoint which of those
// names leak a lib-observability type.
func newIndex(root string) (*index, error) {
	goMod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return nil, err
	}

	match := modulePathPattern.FindSubmatch(goMod)
	if match == nil {
		return nil, fmt.Errorf("no module path in %s/go.mod", root)
	}

	idx := &index{
		modulePath: string(match[1]),
		pkgDir:     map[string]string{},
		pkgName:    map[string]string{},
		decls:      map[string]map[string]typeDecl{},
		leaks:      map[string]map[string]string{},
	}

	fset := token.NewFileSet()

	// Learn every in-module package's declared name before resolving any
	// import table, because an unaliased import of a package whose name
	// differs from its directory is referenced by that name, not by the
	// path's last segment. PackageClauseOnly stops at the package clause,
	// so this pre-walk costs a header read per file.
	err = walkGoFiles(root, func(path, rel string) error {
		file, parseErr := parser.ParseFile(fset, path, nil, parser.PackageClauseOnly)
		if parseErr != nil {
			return parseErr
		}

		dir := filepath.ToSlash(filepath.Dir(rel))
		importPath := idx.importPath(dir)
		idx.pkgDir[importPath] = dir
		idx.pkgName[importPath] = file.Name.Name

		return nil
	})
	if err != nil {
		return nil, err
	}

	err = walkGoFiles(root, func(path, rel string) error {
		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		idx.collectTypeDecls(filepath.ToSlash(filepath.Dir(rel)), file)

		return nil
	})
	if err != nil {
		return nil, err
	}

	idx.resolveLeaks()

	return idx, nil
}

// importPath is the import path a package in dir is reached by. The directory
// alone determines it; the declared package name does not participate, which
// is exactly why pkgName has to be indexed separately.
func (idx *index) importPath(dir string) string {
	if dir == "." {
		return idx.modulePath
	}

	return idx.modulePath + "/" + dir
}

// collectTypeDecls records every package-level type declaration in file.
func (idx *index) collectTypeDecls(dir string, file *ast.File) {
	imports := idx.fileImports(file)

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}

		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}

			if idx.decls[dir] == nil {
				idx.decls[dir] = map[string]typeDecl{}
			}

			idx.decls[dir][typeSpec.Name.Name] = typeDecl{spec: typeSpec, imports: imports}
		}
	}
}

// resolveLeaks iterates the declaration index to a fixpoint. One pass cannot
// suffice: an alias chain (`type a = b; type b = log.Logger`) and a forward
// reference both need the answer for a name that has not been computed yet.
func (idx *index) resolveLeaks() {
	for changed := true; changed; {
		changed = false

		for dir, decls := range idx.decls {
			for name, decl := range decls {
				if _, done := idx.leaks[dir][name]; done {
					continue
				}

				leaked := idx.reachableTypes(decl, dir)
				if len(leaked) == 0 {
					continue
				}

				if idx.leaks[dir] == nil {
					idx.leaks[dir] = map[string]string{}
				}

				idx.leaks[dir][name] = leaked[0]
				changed = true
			}
		}
	}
}

// reachableTypes reports the lib-observability types a consumer can reach
// through decl.
//
// For an alias the whole right-hand side counts: the alias IS that type. For a
// defined type only its externally reachable surface counts - exported fields,
// embedded fields, exported interface methods - because a defined type has its
// own identity, so a defined struct only leaks through the fields a foreigner
// can read: `type pair struct{ L log.Logger }` hands `log.Logger` to anyone
// who can write `x.L`, while its unexported fields hand over nothing.
//
// A defined type over anything else - `type h func(log.Logger)`,
// `type l log.Logger` - counts whole. It errs strict on purpose: writing a
// value for the first means naming the module, and the second inherits the
// method set of the interface it was defined from, whose `With(...) Logger`
// hands a `log.Logger` back to any caller. If a genuinely inert case ever
// shows up here, narrow THIS function; do not add an allowlist entry.
func (idx *index) reachableTypes(decl typeDecl, dir string) []string {
	if decl.spec.Assign.IsValid() {
		return idx.referenced(decl.spec.Type, dir, decl.imports)
	}

	var leaked []string

	reportExportedFields(decl.spec, func(node ast.Node, _ string) {
		leaked = append(leaked, idx.referenced(node, dir, decl.imports)...)
	})

	return leaked
}

// scanFile reports every exported declaration in path whose signature, fields
// or embedded types reach a lib-observability type - directly through an
// import selector, or through a name declared in this module.
func (idx *index) scanFile(path, rel, dir string) ([]violation, error) {
	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}

	imports := idx.fileImports(file)

	var violations []violation

	report := func(node ast.Node, symbol string) {
		for _, typ := range idx.referenced(node, dir, imports) {
			violations = append(violations, violation{
				file:   rel,
				line:   fset.Position(node.Pos()).Line,
				symbol: symbol,
				typ:    typ,
			})
		}
	}

	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if !exportedFunc(d) {
				continue
			}

			report(d.Type, symbolName(d))
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if !s.Name.IsExported() {
						continue
					}

					reportExportedFields(s, report)
				case *ast.ValueSpec:
					for _, name := range s.Names {
						if name.IsExported() && s.Type != nil {
							report(s.Type, name.Name)
						}
					}
				}
			}
		}
	}

	return violations, nil
}

// reportExportedFields reports the exported fields and interface methods of a
// type. Unexported fields are not part of the API surface.
func reportExportedFields(spec *ast.TypeSpec, report func(ast.Node, string)) {
	switch t := spec.Type.(type) {
	case *ast.StructType:
		for _, field := range t.Fields.List {
			if len(field.Names) == 0 {
				report(field.Type, spec.Name.Name+" (embedded)")

				continue
			}

			for _, name := range field.Names {
				if name.IsExported() {
					report(field.Type, spec.Name.Name+"."+name.Name)
				}
			}
		}
	case *ast.InterfaceType:
		for _, method := range t.Methods.List {
			for _, name := range method.Names {
				if name.IsExported() {
					report(method.Type, spec.Name.Name+"."+name.Name)
				}
			}
		}
	default:
		report(spec.Type, spec.Name.Name)
	}
}

// exportedFunc reports whether decl is reachable from outside the package:
// an exported function, or an exported method on an exported receiver.
func exportedFunc(decl *ast.FuncDecl) bool {
	if !decl.Name.IsExported() {
		return false
	}

	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return true
	}

	return receiverIsExported(decl.Recv.List[0].Type)
}

func receiverIsExported(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return receiverIsExported(t.X)
	case *ast.IndexExpr:
		return receiverIsExported(t.X)
	case *ast.IndexListExpr:
		return receiverIsExported(t.X)
	case *ast.Ident:
		return t.IsExported()
	default:
		return false
	}
}

func symbolName(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return decl.Name.Name
	}

	return "(receiver)." + decl.Name.Name
}

// fileImports maps every import of file to its path under the local name it is
// referenced by. Blank and dot imports contribute no name to resolve against.
//
// An unaliased import is referenced by the name in the imported package's
// package clause, which Go only conventionally ties to the last path segment.
// Guessing the segment mis-keys every in-module package that breaks the
// convention - this module has two, `billing/.../v1` declaring `billingv1` and
// the root declaring `streaming` under a `/v3` path - and a mis-keyed entry
// makes the selector resolve to the empty path, i.e. silently to no violation.
// So for imports of this module the declared name comes from the index.
// Third-party paths are not indexed and keep the segment guess.
func (idx *index) fileImports(file *ast.File) map[string]string {
	imports := map[string]string{}

	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}

		name := path[strings.LastIndex(path, "/")+1:]
		if declared, ok := idx.pkgName[path]; ok {
			name = declared
		}

		if imp.Name != nil {
			if imp.Name.Name == "_" || imp.Name.Name == "." {
				continue
			}

			name = imp.Name.Name
		}

		imports[name] = path
	}

	return imports
}

// referenced returns the lib-observability types reachable from anywhere
// inside node, resolved against the import table of the file node came from
// and the package scope of dir.
func (idx *index) referenced(node ast.Node, dir string, imports map[string]string) []string {
	var found []string

	var visit func(ast.Node) bool

	visit = func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.Field:
			// A field's names are not type references; only its type is.
			ast.Inspect(t.Type, visit)

			return false
		case *ast.SelectorExpr:
			ident, ok := t.X.(*ast.Ident)
			if !ok {
				return true
			}

			// A qualified name resolves in the imported package, so the
			// selector must not be re-read as a local name.
			found = append(found, idx.qualified(imports[ident.Name], t.Sel.Name)...)

			return false
		case *ast.Ident:
			if typ, ok := idx.leaks[dir][t.Name]; ok {
				found = append(found, typ+" (via "+t.Name+")")
			}

			return false
		}

		return true
	}

	ast.Inspect(node, visit)

	return found
}

// qualified resolves pkgPath.name: a lib-observability type is a direct hit,
// and a type of this module is a hit when the index says that name leaks.
func (idx *index) qualified(pkgPath, name string) []string {
	if pkgPath == "" {
		return nil
	}

	if strings.HasPrefix(pkgPath, libObservability) {
		return []string{pkgPath + "." + name}
	}

	dir, ok := idx.pkgDir[pkgPath]
	if !ok {
		return nil
	}

	if typ, ok := idx.leaks[dir][name]; ok {
		return []string{typ + " (via " + pkgPath + "." + name + ")"}
	}

	return nil
}

package cmd

// The generation record workflow makes every `sqlc generate` run a
// replayable build input. It is opt-in: nothing in this file runs unless
// the `sqlc record update` or `sqlc record verify` commands are invoked, so
// the existing generate, diff, vet and verify behavior is unchanged when it
// is not used.
//
// `sqlc record update` parses, compiles and runs every package/backend in
// memory, then commits every output file and the record describing their
// inputs and digests as one transaction: a failure of any package or
// backend leaves the previously committed outputs and record in place, and
// files the user maintains in output directories that generation does not
// produce are never deleted or overwritten.
//
// `sqlc record verify` performs the same in-memory run without touching the
// disk and reports:
//
//   - input drift: schema, queries, configuration, plugin or backend
//     options changed since the record was written
//   - output drift: the replay no longer reproduces the recorded outputs
//   - missing: a recorded output file is absent from disk
//   - drift: an output file on disk differs from what the replay produced
//   - conflict: two packages/backends emit the same path with different
//     contents
//   - extra: a file exists in a managed output directory that this
//     generation does not produce (user files are reported, never removed)

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	"github.com/sqlc-dev/sqlc/internal/cache"
	"github.com/sqlc-dev/sqlc/internal/compiler"
	"github.com/sqlc-dev/sqlc/internal/config"
	"github.com/sqlc-dev/sqlc/internal/info"
	"github.com/sqlc-dev/sqlc/internal/plugin"
	"github.com/sqlc-dev/sqlc/internal/sql/sqlpath"
)

const (
	recordManifestVersion = 1
	defaultRecordRelPath  = ".sqlc/record.json"
)

// ---------------------------------------------------------------------------
// Record format
// ---------------------------------------------------------------------------

type recordManifest struct {
	Version     int            `json:"version"`
	SQLCVersion string         `json:"sqlc_version"`
	Config      string         `json:"config"`
	Targets     []recordTarget `json:"targets"`
}

type recordTarget struct {
	ID          string       `json:"id"`
	Package     string       `json:"package,omitempty"`
	Backend     string       `json:"backend"`
	Out         string       `json:"out"`
	Roots       []string     `json:"roots,omitempty"`
	InputDigest cache.Digest `json:"input_digest"`
	Inputs      recordInputs `json:"inputs"`
	Files       []recordFile `json:"files"`
}

type recordInputs struct {
	Engine  string            `json:"engine"`
	Schema  []recordInputFile `json:"schema"`
	Queries []recordInputFile `json:"queries"`
	Plugin  *recordPlugin     `json:"plugin,omitempty"`
}

type recordInputFile struct {
	Path   string       `json:"path"`
	Digest cache.Digest `json:"digest"`
}

type recordPlugin struct {
	Name   string   `json:"name"`
	Kind   string   `json:"kind"`
	Cmd    string   `json:"cmd,omitempty"`
	Format string   `json:"format,omitempty"`
	URL    string   `json:"url,omitempty"`
	SHA256 string   `json:"sha256,omitempty"`
	Env    []string `json:"env,omitempty"`
}

type recordFile struct {
	Path   string       `json:"path"`
	Digest cache.Digest `json:"digest"`
}

// ---------------------------------------------------------------------------
// In-memory generation plan
// ---------------------------------------------------------------------------

type targetSpec struct {
	id      string
	pkg     string
	backend string
	out     string
	plugin  *recordPlugin
	schema  []string
	queries []string
}

type recordedTarget struct {
	spec  targetSpec
	pair  OutputPair
	req   *plugin.GenerateRequest
	files map[string]string // absolute path -> contents
	roots map[string]struct{}
}

type fileOwner struct {
	id      string
	content string
}

type planConflict struct {
	path   string
	owners []string
}

type generationPlan struct {
	dir        string
	configPath string
	targets    []*recordedTarget
	// files is the conflict-checked union of every target's outputs.
	files map[string]string
	// byFile keeps every target claiming an output path, conflicts included.
	byFile map[string][]fileOwner
	roots  map[string]struct{}
	// inputs are the files read as schema/queries plus the config file; they
	// are legitimate residents of an output directory (e.g. out: ".") and
	// must not be reported as extras.
	inputs    map[string]struct{}
	conflicts []planConflict
}

type recordGenerator struct {
	dir     string
	targets []*recordedTarget
}

func backendName(pair OutputPair) string {
	switch {
	case pair.Gen.Go != nil:
		return "go"
	case pair.Gen.JSON != nil:
		return "json"
	case pair.Plugin != nil:
		return "plugin:" + pair.Plugin.Plugin
	default:
		return "unknown"
	}
}

func pluginIdentity(conf *config.Config, name string) *recordPlugin {
	plug, err := findPlugin(*conf, name)
	if err != nil {
		return &recordPlugin{Name: name, Kind: "unknown"}
	}
	rp := &recordPlugin{Name: plug.Name, Env: append([]string(nil), plug.Env...)}
	switch {
	case plug.Process != nil:
		rp.Kind = "process"
		rp.Cmd = plug.Process.Cmd
		rp.Format = plug.Process.Format
	case plug.WASM != nil:
		rp.Kind = "wasm"
		rp.URL = plug.WASM.URL
		rp.SHA256 = plug.WASM.SHA256
	default:
		rp.Kind = "unknown"
	}
	return rp
}

// Pairs mirrors generator.Pairs, assigning each pair a stable id and a
// target slot its result is recorded into.
func (g *recordGenerator) Pairs(ctx context.Context, conf *config.Config) []OutputPair {
	var pairs []OutputPair
	var specs []targetSpec
	for i, sql := range conf.SQL {
		add := func(backend, out, pkg string, gen config.SQLGen, cg *config.Codegen) {
			spec := targetSpec{
				id:      fmt.Sprintf("sql[%d]/%s", i, backend),
				pkg:     pkg,
				backend: backend,
				out:     out,
				schema:  append([]string(nil), sql.Schema...),
				queries: append([]string(nil), sql.Queries...),
			}
			if cg != nil {
				spec.plugin = pluginIdentity(conf, cg.Plugin)
			}
			pairs = append(pairs, OutputPair{
				SQL:    sql,
				Gen:    gen,
				Plugin: cg,
				idx:    len(pairs),
			})
			specs = append(specs, spec)
		}
		if sql.Gen.Go != nil {
			add("go", sql.Gen.Go.Out, sql.Gen.Go.Package, config.SQLGen{Go: sql.Gen.Go}, nil)
		}
		if sql.Gen.JSON != nil {
			add("json", sql.Gen.JSON.Out, sql.Name, config.SQLGen{JSON: sql.Gen.JSON}, nil)
		}
		for j := range sql.Codegen {
			cg := sql.Codegen[j]
			add("plugin:"+cg.Plugin, cg.Out, sql.Name, config.SQLGen{}, &cg)
		}
	}
	g.targets = make([]*recordedTarget, len(pairs))
	for i := range pairs {
		g.targets[i] = &recordedTarget{
			spec:  specs[i],
			pair:  pairs[i],
			files: map[string]string{},
			roots: map[string]struct{}{},
		}
	}
	return pairs
}

func (g *recordGenerator) ProcessResult(ctx context.Context, combo config.CombinedSettings, pair OutputPair, result *compiler.Result) error {
	out, req, resp, err := codegen(ctx, combo, pair, result)
	if err != nil {
		return err
	}
	t := g.targets[pair.idx]
	t.req = req
	for root := range outputRoots(g.dir, out, pair) {
		t.roots[root] = struct{}{}
	}
	for _, file := range resp.Files {
		filename, err := outputFileAbsPath(g.dir, out, pair, file.Name)
		if err != nil {
			return err
		}
		t.files[filename] = string(file.Contents)
	}
	return nil
}

// runRecordGeneration performs the shared parse/compile/codegen phase used
// by both update and verify. Nothing is written to disk by this function;
// any failure of any package or backend is returned before a plan exists.
func runRecordGeneration(ctx context.Context, dir, filename string, o *Options) (string, *config.Config, *generationPlan, error) {
	e := o.Env
	stderr := o.Stderr

	configPath, conf, err := o.ReadConfig(dir, filename)
	if err != nil {
		return "", nil, nil, err
	}

	base := filepath.Base(configPath)
	if err := config.Validate(conf); err != nil {
		fmt.Fprintf(stderr, "error validating %s: %s\n", base, err)
		return "", nil, nil, err
	}
	if err := e.Validate(conf); err != nil {
		fmt.Fprintf(stderr, "error validating %s: %s\n", base, err)
		return "", nil, nil, err
	}

	g := &recordGenerator{dir: dir}
	if err := processQuerySets(ctx, g, conf, dir, o); err != nil {
		return "", nil, nil, err
	}

	return configPath, conf, g.build(configPath), nil
}

func (g *recordGenerator) build(configPath string) *generationPlan {
	plan := &generationPlan{
		dir:        g.dir,
		configPath: configPath,
		targets:    g.targets,
		files:      map[string]string{},
		byFile:     map[string][]fileOwner{},
		roots:      map[string]struct{}{},
		inputs:     map[string]struct{}{},
	}
	plan.inputs[filepath.Clean(configPath)] = struct{}{}
	for _, t := range g.targets {
		for root := range t.roots {
			plan.roots[root] = struct{}{}
		}
		for path, content := range t.files {
			plan.byFile[path] = append(plan.byFile[path], fileOwner{id: t.spec.id, content: content})
		}
		for _, globs := range [][]string{t.spec.schema, t.spec.queries} {
			var joined []string
			for _, p := range globs {
				joined = append(joined, filepath.Join(g.dir, p))
			}
			files, err := sqlpath.Glob(joined)
			if err != nil {
				continue
			}
			for _, f := range files {
				plan.inputs[filepath.Clean(f)] = struct{}{}
			}
		}
	}
	for path, owners := range plan.byFile {
		plan.files[path] = owners[0].content
		for i := 1; i < len(owners); i++ {
			if owners[i].content != owners[0].content {
				ids := make([]string, len(owners))
				for j, o := range owners {
					ids[j] = o.id
				}
				sort.Strings(ids)
				plan.conflicts = append(plan.conflicts, planConflict{path: path, owners: ids})
				break
			}
		}
	}
	sort.Slice(plan.conflicts, func(i, j int) bool { return plan.conflicts[i].path < plan.conflicts[j].path })
	return plan
}

// ---------------------------------------------------------------------------
// Input identity
// ---------------------------------------------------------------------------

// relPath makes a project-relative path when possible, leaving paths
// outside the project directory untouched. The recorded identity must not
// depend on the absolute location of the checkout.
func relPath(base, p string) string {
	if r, err := filepath.Rel(base, p); err == nil && r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return filepath.ToSlash(r)
	}
	return filepath.ToSlash(p)
}

// canonicalRequestDigest is the stable input identity of a target: a
// deterministic serialization of the complete codegen request, which
// captures the parsed catalog and queries (schema and query contents), the
// engine, every backend/plugin option and the plugin definition. Absolute
// paths and the sqlc version are stripped so the digest is portable and
// only tracks declared inputs; a tooling upgrade surfaces as output drift.
func canonicalRequestDigest(dir string, req *plugin.GenerateRequest) (cache.Digest, error) {
	clone := proto.Clone(req).(*plugin.GenerateRequest)
	clone.SqlcVersion = ""
	if clone.Settings != nil {
		for i, p := range clone.Settings.Schema {
			clone.Settings.Schema[i] = relPath(dir, p)
		}
		for i, p := range clone.Settings.Queries {
			clone.Settings.Queries[i] = relPath(dir, p)
		}
	}
	for _, q := range clone.Queries {
		q.Filename = relPath(dir, q.Filename)
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(clone)
	if err != nil {
		return cache.Digest{}, err
	}
	return cache.DigestOf(b), nil
}

func digestInputFiles(dir string, globs []string) ([]recordInputFile, error) {
	var joined []string
	for _, p := range globs {
		joined = append(joined, filepath.Join(dir, p))
	}
	files, err := sqlpath.Glob(joined)
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	out := make([]recordInputFile, 0, len(files))
	for _, f := range files {
		blob, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		out = append(out, recordInputFile{Path: relPath(dir, f), Digest: cache.DigestOf(blob)})
	}
	return out, nil
}

func (p *generationPlan) manifest(configName string) (*recordManifest, error) {
	man := &recordManifest{
		Version:     recordManifestVersion,
		SQLCVersion: info.Version,
		Config:      configName,
		Targets:     make([]recordTarget, 0, len(p.targets)),
	}
	for _, t := range p.targets {
		schema, err := digestInputFiles(p.dir, t.spec.schema)
		if err != nil {
			return nil, err
		}
		queries, err := digestInputFiles(p.dir, t.spec.queries)
		if err != nil {
			return nil, err
		}
		inputDigest, err := canonicalRequestDigest(p.dir, t.req)
		if err != nil {
			return nil, err
		}
		var roots []string
		for root := range t.roots {
			roots = append(roots, relPath(p.dir, root))
		}
		sort.Strings(roots)
		files := make([]recordFile, 0, len(t.files))
		for path, content := range t.files {
			files = append(files, recordFile{Path: relPath(p.dir, path), Digest: cache.DigestOf([]byte(content))})
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
		engine := ""
		if t.req.GetSettings() != nil {
			engine = t.req.GetSettings().GetEngine()
		}
		man.Targets = append(man.Targets, recordTarget{
			ID:          t.spec.id,
			Package:     t.spec.pkg,
			Backend:     t.spec.backend,
			Out:         t.spec.out,
			Roots:       roots,
			InputDigest: inputDigest,
			Inputs: recordInputs{
				Engine:  engine,
				Schema:  schema,
				Queries: queries,
				Plugin:  t.spec.plugin,
			},
			Files: files,
		})
	}
	return man, nil
}

func encodeManifest(man *recordManifest) ([]byte, error) {
	blob, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(blob, '\n'), nil
}

func loadRecord(path string) (*recordManifest, error) {
	blob, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var man recordManifest
	if err := json.Unmarshal(blob, &man); err != nil {
		return nil, fmt.Errorf("parse record %s: %w", path, err)
	}
	if man.Version != recordManifestVersion {
		return nil, fmt.Errorf("unsupported record version %d in %s (expected %d)", man.Version, path, recordManifestVersion)
	}
	return &man, nil
}

func resolveRecordPath(dir, override string, conf *config.Config) string {
	p := override
	if p == "" && conf != nil {
		p = conf.Record.Path
	}
	if p == "" {
		p = defaultRecordRelPath
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(dir, p)
	}
	return filepath.Clean(p)
}

// ---------------------------------------------------------------------------
// Atomic commit with rollback
// ---------------------------------------------------------------------------

// missingAncestors returns directory components above path that do not exist
// yet, deepest first, so a rollback can remove exactly what the commit
// created.
func missingAncestors(path string) []string {
	var chain []string
	d := filepath.Dir(path)
	for {
		if _, err := os.Stat(d); err == nil {
			break
		}
		chain = append(chain, d)
		parent := filepath.Dir(d)
		if parent == d {
			break
		}
		d = parent
	}
	return chain
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".sqlc-record-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			os.Remove(tmpName)
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Chmod(tmpName, mode); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

type committedFile struct {
	path    string
	existed bool
	oldMode os.FileMode
	oldData []byte
}

// commitPlan writes all generated outputs and then the record, as one
// transaction. Any failure restores the previous state: overwritten files
// are restored with their original contents and mode, created files are
// removed and freshly created empty directories are cleaned up. Files not
// produced by the generation are never touched.
func commitPlan(plan *generationPlan, recordPath string, recordData []byte) (err error) {
	var (
		changed     []*committedFile
		createdDirs []string
		trackedDirs = map[string]struct{}{}
	)
	addDirs := func(path string) {
		for _, d := range missingAncestors(path) {
			if _, ok := trackedDirs[d]; !ok {
				trackedDirs[d] = struct{}{}
				createdDirs = append(createdDirs, d)
			}
		}
	}
	rollback := func(stage string, cause error) error {
		for i := len(changed) - 1; i >= 0; i-- {
			c := changed[i]
			if c.existed {
				_ = writeFileAtomic(c.path, c.oldData, c.oldMode)
			} else {
				_ = os.Remove(c.path)
			}
		}
		sort.Slice(createdDirs, func(i, j int) bool { return len(createdDirs[i]) > len(createdDirs[j]) })
		for _, d := range createdDirs {
			_ = os.Remove(d)
		}
		return fmt.Errorf("record commit failed at %s: %w (all changes rolled back)", stage, cause)
	}

	paths := make([]string, 0, len(plan.files))
	for path := range plan.files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		content := []byte(plan.files[path])
		if old, err := os.ReadFile(path); err == nil && bytes.Equal(old, content) {
			continue
		}
		addDirs(path)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return rollback("mkdir", err)
		}
		c := &committedFile{path: path}
		if info, err := os.Stat(path); err == nil {
			old, rerr := os.ReadFile(path)
			if rerr != nil {
				return rollback("backup", rerr)
			}
			c.existed = true
			c.oldMode = info.Mode().Perm()
			c.oldData = old
		} else if !errors.Is(err, fs.ErrNotExist) {
			return rollback("stat", err)
		}
		mode := os.FileMode(0644)
		if c.existed {
			mode = c.oldMode
		}
		if err := writeFileAtomic(path, content, mode); err != nil {
			return rollback("write", err)
		}
		changed = append(changed, c)
	}

	// The record is committed last. Until this rename succeeds the previous
	// record is still the last complete successful state.
	addDirs(recordPath)
	if err := os.MkdirAll(filepath.Dir(recordPath), 0755); err != nil {
		return rollback("record mkdir", err)
	}
	recordChange := &committedFile{path: recordPath}
	if info, err := os.Stat(recordPath); err == nil {
		old, rerr := os.ReadFile(recordPath)
		if rerr != nil {
			return rollback("record backup", rerr)
		}
		recordChange.existed = true
		recordChange.oldMode = info.Mode().Perm()
		recordChange.oldData = old
	} else if !errors.Is(err, fs.ErrNotExist) {
		return rollback("record stat", err)
	}
	recordMode := os.FileMode(0644)
	if recordChange.existed {
		recordMode = recordChange.oldMode
	}
	if err := writeFileAtomic(recordPath, recordData, recordMode); err != nil {
		return rollback("record write", err)
	}
	changed = append(changed, recordChange)
	return nil
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

// UpdateSummary reports what an update committed.
type UpdateSummary struct {
	RecordPath string
	Targets    int
	Files      int
}

var ErrRecordConflict = errors.New("conflicting generated outputs")

// RecordUpdate runs the full generation and commits outputs plus the record
// as one transaction.
func RecordUpdate(ctx context.Context, dir, filename string, o *Options, recordOverride string) (*UpdateSummary, error) {
	configPath, conf, plan, err := runRecordGeneration(ctx, dir, filename, o)
	if err != nil {
		return nil, err
	}
	if len(plan.conflicts) > 0 {
		stderr := o.Stderr
		for _, c := range plan.conflicts {
			fmt.Fprintf(stderr, "CONFLICT\t%s\t%s\n", relPath(dir, c.path), strings.Join(c.owners, ", "))
		}
		return nil, fmt.Errorf("%w: %d path(s) generated with different contents by multiple targets; nothing was written", ErrRecordConflict, len(plan.conflicts))
	}
	man, err := plan.manifest(filepath.Base(configPath))
	if err != nil {
		return nil, err
	}
	blob, err := encodeManifest(man)
	if err != nil {
		return nil, err
	}
	recordPath := resolveRecordPath(dir, recordOverride, conf)
	if err := commitPlan(plan, recordPath, blob); err != nil {
		fmt.Fprintf(o.Stderr, "%s\n", err)
		return nil, err
	}
	return &UpdateSummary{
		RecordPath: recordPath,
		Targets:    len(plan.targets),
		Files:      len(plan.files),
	}, nil
}

// FindingKind classifies a verify finding.
type FindingKind string

const (
	FindingInputDrift FindingKind = "input-drift"
	FindingMissing    FindingKind = "missing"
	FindingDrift      FindingKind = "drift"
	FindingConflict   FindingKind = "conflict"
	FindingExtra      FindingKind = "extra"
)

// Finding is one discrepancy reported by RecordVerify.
type Finding struct {
	Kind   FindingKind
	Target string
	Path   string
	Detail string
}

// Report is the read-only result of RecordVerify.
type Report struct {
	RecordPath string
	Findings   []Finding
	Targets    int
	Files      int
}

// OK reports whether the replayed generation matches the record and disk.
func (r *Report) OK() bool { return len(r.Findings) == 0 }

// Print writes a deterministic, human-readable report to w.
func (r *Report) Print(w io.Writer) {
	global := make([]Finding, 0)
	byTarget := map[string][]Finding{}
	var order []string
	for _, f := range r.Findings {
		if f.Target == "" {
			global = append(global, f)
			continue
		}
		if _, ok := byTarget[f.Target]; !ok {
			order = append(order, f.Target)
		}
		byTarget[f.Target] = append(byTarget[f.Target], f)
	}
	sort.Strings(order)
	for _, id := range order {
		fmt.Fprintf(w, "# target %s\n", id)
		for _, f := range byTarget[id] {
			printFinding(w, f)
		}
	}
	if len(global) > 0 {
		fmt.Fprintln(w, "# output directories")
		for _, f := range global {
			printFinding(w, f)
		}
	}
	if r.OK() {
		fmt.Fprintf(w, "ok\t%s: %d target(s), %d file(s) match the record\n", r.RecordPath, r.Targets, r.Files)
	} else {
		fmt.Fprintf(w, "FAIL\t%s: %d problem(s) found; review the changes, then run `sqlc record update` to replace the record\n", r.RecordPath, len(r.Findings))
	}
}

func printFinding(w io.Writer, f Finding) {
	switch f.Kind {
	case FindingInputDrift:
		fmt.Fprintf(w, "INPUT DRIFT\t%s\n", f.Detail)
	case FindingMissing:
		fmt.Fprintf(w, "MISSING\t%s\n", f.Path)
	case FindingDrift:
		fmt.Fprintf(w, "DRIFT\t%s\t%s\n", f.Path, f.Detail)
	case FindingConflict:
		fmt.Fprintf(w, "CONFLICT\t%s\t%s\n", f.Path, f.Detail)
	case FindingExtra:
		fmt.Fprintf(w, "EXTRA\t%s\t%s\n", f.Path, f.Detail)
	}
}

// RecordVerify replays generation in memory and compares it against the
// committed record and the files on disk. It never modifies anything.
func RecordVerify(ctx context.Context, dir, filename string, o *Options, recordOverride string) (*Report, error) {
	_, conf, plan, err := runRecordGeneration(ctx, dir, filename, o)
	if err != nil {
		return nil, err
	}
	recordPath := resolveRecordPath(dir, recordOverride, conf)
	man, err := loadRecord(recordPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("no generation record at %s; run `sqlc record update` first", relPath(dir, recordPath))
		}
		return nil, err
	}

	report := &Report{RecordPath: relPath(dir, recordPath), Targets: len(plan.targets), Files: len(plan.files)}
	add := func(kind FindingKind, target, path, detail string) {
		report.Findings = append(report.Findings, Finding{Kind: kind, Target: target, Path: path, Detail: detail})
	}

	recorded := map[string]*recordTarget{}
	for i := range man.Targets {
		recorded[man.Targets[i].ID] = &man.Targets[i]
	}
	fresh := map[string]*recordedTarget{}
	for _, t := range plan.targets {
		fresh[t.spec.id] = t
	}

	for _, c := range plan.conflicts {
		add(FindingConflict, "", relPath(dir, c.path),
			fmt.Sprintf("generated with different contents by %s", strings.Join(c.owners, " and ")))
	}

	// Target-level identity and output comparison.
	var ids []string
	for id := range fresh {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := fresh[id]
		rt, ok := recorded[id]
		if !ok {
			add(FindingInputDrift, id, "", "target is not present in the record")
			continue
		}
		dg, err := canonicalRequestDigest(dir, t.req)
		if err != nil {
			return nil, err
		}
		if dg.Hash != rt.InputDigest.Hash {
			add(FindingInputDrift, id, "", "schema, queries, configuration, plugin or backend options changed since the record was written")
		}
		recordedFiles := map[string]cache.Digest{}
		for _, f := range rt.Files {
			recordedFiles[f.Path] = f.Digest
		}
		freshFiles := map[string]cache.Digest{}
		for path, content := range t.files {
			freshFiles[relPath(dir, path)] = cache.DigestOf([]byte(content))
		}
		for path, digest := range recordedFiles {
			got, ok := freshFiles[path]
			switch {
			case !ok:
				add(FindingDrift, id, path, "recorded output is no longer generated")
			case got.Hash != digest.Hash:
				add(FindingDrift, id, path, "replayed output does not match the record")
			}
		}
		for path := range freshFiles {
			if _, ok := recordedFiles[path]; !ok {
				add(FindingDrift, id, path, "generated output is not in the record")
			}
		}
	}
	for id := range recorded {
		if _, ok := fresh[id]; !ok {
			add(FindingInputDrift, id, "", "recorded target is missing from the current configuration")
		}
	}

	// On-disk state of every file the replay would produce.
	diskPaths := make([]string, 0, len(plan.files))
	for path := range plan.files {
		diskPaths = append(diskPaths, path)
	}
	sort.Strings(diskPaths)
	owner := func(path string) string {
		if owners, ok := plan.byFile[path]; ok && len(owners) > 0 {
			return owners[0].id
		}
		return ""
	}
	for _, path := range diskPaths {
		rel := relPath(dir, path)
		blob, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				add(FindingMissing, owner(path), rel, "")
				continue
			}
			return nil, err
		}
		if cache.DigestOf(blob).Hash != cache.DigestOf([]byte(plan.files[path])).Hash {
			add(FindingDrift, owner(path), rel, "file on disk differs from the replayed generation")
		}
	}

	// Extra files: regular files under managed output roots that this
	// generation does not produce. They are reported, never removed.
	recordedAll := map[string]struct{}{}
	for _, t := range man.Targets {
		for _, f := range t.Files {
			recordedAll[f.Path] = struct{}{}
		}
	}
	var roots []string
	for root := range plan.roots {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	seen := map[string]struct{}{}
	for _, root := range roots {
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if path == root {
				return nil
			}
			name := d.Name()
			if strings.HasPrefix(name, ".") {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if d.IsDir() {
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			clean := filepath.Clean(path)
			if _, ok := seen[clean]; ok {
				return nil
			}
			if clean == filepath.Clean(recordPath) {
				return nil
			}
			if _, ok := plan.inputs[clean]; ok {
				return nil
			}
			if _, ok := plan.files[clean]; ok {
				return nil
			}
			seen[clean] = struct{}{}
			rel := relPath(dir, clean)
			if _, wasRecorded := recordedAll[rel]; wasRecorded {
				add(FindingExtra, "", rel, "file was recorded previously but is no longer generated; it was left in place")
			} else {
				add(FindingExtra, "", rel, "untracked file in an output directory; not produced by this generation and left in place")
			}
			return nil
		})
	}

	sort.SliceStable(report.Findings, func(i, j int) bool {
		a, b := report.Findings[i], report.Findings[j]
		if a.Target != b.Target {
			return a.Target < b.Target
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Kind < b.Kind
	})
	return report, nil
}

// ---------------------------------------------------------------------------
// CLI commands
// ---------------------------------------------------------------------------

var recordCmd = &cobra.Command{
	Use:   "record",
	Short: "Record and verify reproducible generation inputs and outputs",
	Long: `Manage the generation record, an opt-in manifest of the stable input
identity, plugin/options information and output file digests for every
package and generation backend in the configuration.

  sqlc record update   Generate everything and atomically replace the record
  sqlc record verify   Replay generation without writing and report drift`,
}

var recordUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Generate code and atomically replace the generation record",
	RunE: func(cmd *cobra.Command, args []string) error {
		stderr := cmd.ErrOrStderr()
		dir, name := getConfigPath(stderr, cmd.Flag("file"))
		override, _ := cmd.Flags().GetString("path")
		summary, err := RecordUpdate(cmd.Context(), dir, name, &Options{
			Env:    ParseEnv(cmd),
			Stderr: stderr,
		}, override)
		if err != nil {
			os.Exit(1)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "record updated: %s (%d target(s), %d output file(s))\n",
			relPath(dir, summary.RecordPath), summary.Targets, summary.Files)
		return nil
	},
}

var recordVerifyCmd = &cobra.Command{
	Use:     "verify",
	Aliases: []string{"check"},
	Short:   "Replay generation without writing files and verify the record",
	RunE: func(cmd *cobra.Command, args []string) error {
		stderr := cmd.ErrOrStderr()
		dir, name := getConfigPath(stderr, cmd.Flag("file"))
		override, _ := cmd.Flags().GetString("path")
		report, err := RecordVerify(cmd.Context(), dir, name, &Options{
			Env:    ParseEnv(cmd),
			Stderr: stderr,
		}, override)
		if err != nil {
			fmt.Fprintf(stderr, "Error verifying generation record: %s\n", err)
			os.Exit(1)
		}
		report.Print(stderr)
		if !report.OK() {
			os.Exit(1)
		}
		return nil
	},
}

func init() {
	recordCmd.PersistentFlags().String("path", "", "path to the generation record (default: .sqlc/record.json)")
	recordCmd.AddCommand(recordUpdateCmd)
	recordCmd.AddCommand(recordVerifyCmd)
}

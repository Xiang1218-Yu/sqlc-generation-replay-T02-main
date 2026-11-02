package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The record workflow is exercised through the same entry points the CLI
// uses, against on-disk projects in temp directories: it is a file-level
// contract (inputs, generated outputs and the record) that cannot be pinned
// by comparing in-memory values alone.

func writeRecordProject(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func recordOptions() (*Options, *bytes.Buffer) {
	stderr := &bytes.Buffer{}
	return &Options{Stderr: stderr}, stderr
}

func mustUpdate(t *testing.T, dir string) *UpdateSummary {
	t.Helper()
	opts, _ := recordOptions()
	summary, err := RecordUpdate(context.Background(), dir, "", opts, "")
	if err != nil {
		t.Fatalf("RecordUpdate failed: %v", err)
	}
	return summary
}

func mustVerify(t *testing.T, dir string) *Report {
	t.Helper()
	opts, _ := recordOptions()
	report, err := RecordVerify(context.Background(), dir, "", opts, "")
	if err != nil {
		t.Fatalf("RecordVerify failed: %v", err)
	}
	return report
}

func readRecord(t *testing.T, dir string) *recordManifest {
	t.Helper()
	man, err := loadRecord(filepath.Join(dir, ".sqlc", "record.json"))
	if err != nil {
		t.Fatal(err)
	}
	return man
}

func hasFinding(r *Report, kind FindingKind, path string) bool {
	for _, f := range r.Findings {
		if f.Kind == kind && f.Path == path {
			return true
		}
	}
	return false
}

const sqliteSchema = `CREATE TABLE authors (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
`

const sqliteQuery = `-- name: GetAuthor :one
SELECT * FROM authors WHERE id = ?;
`

func singlePackageConfig(out string) string {
	return "version: \"2\"\n" +
		"sql:\n" +
		"  - engine: \"sqlite\"\n" +
		"    schema: \"schema.sql\"\n" +
		"    queries: \"query.sql\"\n" +
		"    gen:\n" +
		"      go:\n" +
		"        out: \"" + out + "\"\n" +
		"        package: \"" + filepath.Base(out) + "\"\n"
}

func TestRecordUpdateCreatesOutputsAndRecord(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})

	summary := mustUpdate(t, dir)
	if summary.Targets != 1 || summary.Files == 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	for _, name := range []string{"db/models.go", "db/query.sql.go", ".sqlc/record.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s to exist: %v", name, err)
		}
	}

	man := readRecord(t, dir)
	if man.Version != recordManifestVersion {
		t.Fatalf("record version = %d, want %d", man.Version, recordManifestVersion)
	}
	if man.SQLCVersion == "" {
		t.Fatal("record does not capture the sqlc version")
	}
	if len(man.Targets) != 1 {
		t.Fatalf("targets = %d, want 1", len(man.Targets))
	}
	target := man.Targets[0]
	if target.ID != "sql[0]/go" || target.Backend != "go" || target.Out != "db" {
		t.Fatalf("unexpected target: %+v", target)
	}
	if target.Inputs.Engine != "sqlite" {
		t.Fatalf("engine = %q", target.Inputs.Engine)
	}
	if len(target.Inputs.Schema) != 1 || target.Inputs.Schema[0].Path != "schema.sql" {
		t.Fatalf("schema inputs = %+v", target.Inputs.Schema)
	}
	if len(target.Inputs.Queries) != 1 || target.Inputs.Queries[0].Path != "query.sql" {
		t.Fatalf("query inputs = %+v", target.Inputs.Queries)
	}
	if len(target.InputDigest.Hash) != 64 {
		t.Fatalf("input digest = %+v", target.InputDigest)
	}
	var hasModels, hasQuery bool
	for _, f := range target.Files {
		if len(f.Digest.Hash) != 64 || f.Digest.SizeBytes <= 0 {
			t.Fatalf("bad file digest: %+v", f.Digest)
		}
		hasModels = hasModels || f.Path == "db/models.go"
		hasQuery = hasQuery || f.Path == "db/query.sql.go"
	}
	if !hasModels || !hasQuery {
		t.Fatalf("recorded files = %+v", target.Files)
	}

	if r := mustVerify(t, dir); !r.OK() {
		t.Fatalf("verify after update reported findings: %+v", r.Findings)
	}

	// Re-running update with no changes must reproduce the record byte for
	// byte, so the workflow is itself replayable.
	first, err := os.ReadFile(filepath.Join(dir, ".sqlc", "record.json"))
	if err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, dir)
	second, err := os.ReadFile(filepath.Join(dir, ".sqlc", "record.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("record is not deterministic across identical updates")
	}
}

func TestRecordMultipleBackends(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml": `version: "2"
sql:
  - engine: "sqlite"
    schema: "schema.sql"
    queries: "query.sql"
    gen:
      go:
        out: "db"
        package: "db"
      json:
        out: "dbjson"
`,
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})

	mustUpdate(t, dir)

	man := readRecord(t, dir)
	var backends []string
	for _, target := range man.Targets {
		backends = append(backends, target.ID)
	}
	if len(backends) != 2 || backends[0] != "sql[0]/go" || backends[1] != "sql[0]/json" {
		t.Fatalf("targets = %v", backends)
	}
	if _, err := os.Stat(filepath.Join(dir, "dbjson", "codegen_request.json")); err != nil {
		t.Fatalf("json output missing: %v", err)
	}
	if r := mustVerify(t, dir); !r.OK() {
		t.Fatalf("verify: %+v", r.Findings)
	}
}

func TestRecordMultiplePackages(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml": `version: "2"
sql:
  - engine: "sqlite"
    schema: "a/schema.sql"
    queries: "a/query.sql"
    gen:
      go: { out: "a/out", package: "a" }
  - engine: "sqlite"
    schema: "b/schema.sql"
    queries: "b/query.sql"
    gen:
      go: { out: "b/out", package: "b" }
`,
		"a/schema.sql": sqliteSchema,
		"a/query.sql":  sqliteQuery,
		"b/schema.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"b/query.sql":  "-- name: ListWidgets :many\nSELECT * FROM widgets;\n",
	})

	mustUpdate(t, dir)
	man := readRecord(t, dir)
	if len(man.Targets) != 2 {
		t.Fatalf("targets = %d, want 2", len(man.Targets))
	}
	if man.Targets[0].ID != "sql[0]/go" || man.Targets[1].ID != "sql[1]/go" {
		t.Fatalf("ids = %s, %s", man.Targets[0].ID, man.Targets[1].ID)
	}
	if r := mustVerify(t, dir); !r.OK() {
		t.Fatalf("verify: %+v", r.Findings)
	}
}

func TestRecordExtraUserFilesAreReportedNotDeleted(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})
	mustUpdate(t, dir)

	userFile := filepath.Join(dir, "db", "HANDWRITTEN.go")
	if err := os.WriteFile(userFile, []byte("package db\n\n// hand kept\n"), 0644); err != nil {
		t.Fatal(err)
	}

	r := mustVerify(t, dir)
	if r.OK() {
		t.Fatal("verify should flag the extra file")
	}
	if !hasFinding(r, FindingExtra, "db/HANDWRITTEN.go") {
		t.Fatalf("findings = %+v", r.Findings)
	}

	// Update must neither delete nor modify the user file.
	before, _ := os.ReadFile(userFile)
	mustUpdate(t, dir)
	after, err := os.ReadFile(userFile)
	if err != nil {
		t.Fatalf("user file disappeared after update: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("user file was modified by update")
	}
}

func TestRecordMissingAndDrift(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})
	mustUpdate(t, dir)

	models := filepath.Join(dir, "db", "models.go")
	if err := os.Remove(models); err != nil {
		t.Fatal(err)
	}
	queryFile := filepath.Join(dir, "db", "query.sql.go")
	if err := os.WriteFile(queryFile, []byte("// tampered\n"), 0644); err != nil {
		t.Fatal(err)
	}

	r := mustVerify(t, dir)
	if !hasFinding(r, FindingMissing, "db/models.go") {
		t.Fatalf("missing finding absent: %+v", r.Findings)
	}
	var driftPath string
	for _, f := range r.Findings {
		if f.Kind == FindingDrift && f.Path == "db/query.sql.go" {
			driftPath = f.Path
		}
	}
	if driftPath == "" {
		t.Fatalf("drift finding absent: %+v", r.Findings)
	}

	// Explicit update repairs the tree and converges back to a clean state.
	mustUpdate(t, dir)
	if r := mustVerify(t, dir); !r.OK() {
		t.Fatalf("verify after repair: %+v", r.Findings)
	}
}

func TestRecordInputDrift(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})
	mustUpdate(t, dir)

	// A committed schema file changes after the record was written.
	if err := os.WriteFile(filepath.Join(dir, "schema.sql"),
		[]byte("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT NOT NULL, bio TEXT);\n"),
		0644); err != nil {
		t.Fatal(err)
	}

	r := mustVerify(t, dir)
	var sawInputDrift bool
	for _, f := range r.Findings {
		if f.Kind == FindingInputDrift && f.Target == "sql[0]/go" {
			sawInputDrift = true
		}
	}
	if !sawInputDrift {
		t.Fatalf("expected input drift, findings = %+v", r.Findings)
	}

	// Replacing the record explicitly clears the report.
	mustUpdate(t, dir)
	if r := mustVerify(t, dir); !r.OK() {
		t.Fatalf("verify after update: %+v", r.Findings)
	}
}

func TestRecordConflictWritesNothing(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml": `version: "2"
sql:
  - engine: "sqlite"
    schema: "schema_a.sql"
    queries: "query_a.sql"
    gen:
      go: { out: "shared", package: "shared" }
  - engine: "sqlite"
    schema: "schema_b.sql"
    queries: "query_b.sql"
    gen:
      go: { out: "shared", package: "shared" }
`,
		"schema_a.sql": sqliteSchema,
		"query_a.sql":  sqliteQuery,
		"schema_b.sql": "CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT NOT NULL);\n",
		"query_b.sql":  "-- name: GetWidget :one\nSELECT * FROM widgets WHERE id = ?;\n",
	})

	opts, _ := recordOptions()
	_, err := RecordUpdate(context.Background(), dir, "", opts, "")
	if !errors.Is(err, ErrRecordConflict) {
		t.Fatalf("error = %v, want ErrRecordConflict", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "shared")); !os.IsNotExist(err) {
		t.Fatalf("output directory was created despite conflict: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".sqlc")); !os.IsNotExist(err) {
		t.Fatalf("record directory was created despite conflict: %v", err)
	}
}

func TestRecordFailureConvergesFromLastGoodState(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})
	mustUpdate(t, dir)

	goodRecord, err := os.ReadFile(filepath.Join(dir, ".sqlc", "record.json"))
	if err != nil {
		t.Fatal(err)
	}
	goodModels, err := os.ReadFile(filepath.Join(dir, "db", "models.go"))
	if err != nil {
		t.Fatal(err)
	}

	// Add a second package that fails to parse while the first package is
	// still generated successfully in memory.
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml": `version: "2"
sql:
  - engine: "sqlite"
    schema: "schema.sql"
    queries: "query.sql"
    gen:
      go: { out: "db", package: "db" }
  - engine: "sqlite"
    schema: "broken.sql"
    queries: "broken_query.sql"
    gen:
      go: { out: "broken", package: "broken" }
`,
		"broken.sql":       "CREATE TABLE broken (;\n",
		"broken_query.sql": "-- name: Broken :one\nSELECT FROM broken;\n",
	})

	opts, _ := recordOptions()
	if _, err := RecordUpdate(context.Background(), dir, "", opts, ""); err == nil {
		t.Fatal("expected update to fail on broken package")
	}

	// No partial outputs and the last complete success is untouched.
	if _, err := os.Stat(filepath.Join(dir, "broken")); !os.IsNotExist(err) {
		t.Fatal("partial output directory from failed run was left behind")
	}
	stillGoodRecord, err := os.ReadFile(filepath.Join(dir, ".sqlc", "record.json"))
	if err != nil || !bytes.Equal(goodRecord, stillGoodRecord) {
		t.Fatal("record changed despite a failed generation")
	}
	stillGoodModels, err := os.ReadFile(filepath.Join(dir, "db", "models.go"))
	if err != nil || !bytes.Equal(goodModels, stillGoodModels) {
		t.Fatal("existing outputs changed despite a failed generation")
	}

	// After fixing the configuration, rerunning update converges cleanly.
	writeRecordProject(t, dir, map[string]string{"sqlc.yaml": singlePackageConfig("db")})
	mustUpdate(t, dir)
	if r := mustVerify(t, dir); !r.OK() {
		t.Fatalf("verify after recovery: %+v", r.Findings)
	}
}

func TestRecordVerifyWithoutRecord(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})
	opts, _ := recordOptions()
	_, err := RecordVerify(context.Background(), dir, "", opts, "")
	if err == nil || !strings.Contains(err.Error(), "no generation record") {
		t.Fatalf("error = %v, want missing record error", err)
	}
}

func TestRecordWorkflowDoesNotChangeGenerate(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})

	// With the workflow never enabled, generate behaves as before: it
	// returns outputs in memory and writes no record of any kind.
	opts, _ := recordOptions()
	output, err := Generate(context.Background(), dir, "", opts)
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if len(output) == 0 {
		t.Fatal("generate produced no outputs")
	}
	if _, ok := output[filepath.Join(dir, "db", "models.go")]; !ok {
		t.Fatalf("models.go missing from generate output: %v", output)
	}
	if _, err := os.Stat(filepath.Join(dir, ".sqlc")); !os.IsNotExist(err) {
		t.Fatal("generate created a record directory")
	}
}

func TestRecordRecordedFileNoLongerGenerated(t *testing.T) {
	dir := t.TempDir()
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml":  singlePackageConfig("db"),
		"schema.sql": sqliteSchema,
		"query.sql":  sqliteQuery,
	})
	mustUpdate(t, dir)

	// Rename the query file: the new run emits other.sql.go instead, so the
	// recorded query.sql.go is no longer generated. Verify flags it both as
	// drift against the record and as a leftover file on disk; update must
	// never delete it.
	writeRecordProject(t, dir, map[string]string{
		"sqlc.yaml": `version: "2"
sql:
  - engine: "sqlite"
    schema: "schema.sql"
    queries: "other.sql"
    gen:
      go: { out: "db", package: "db" }
`,
		"other.sql": sqliteQuery,
	})

	r := mustVerify(t, dir)
	var drift, extra bool
	for _, f := range r.Findings {
		drift = drift || (f.Kind == FindingDrift && f.Path == "db/query.sql.go")
		extra = extra || (f.Kind == FindingExtra && f.Path == "db/query.sql.go")
	}
	if !drift || !extra {
		t.Fatalf("expected drift+extra for stale query.sql.go, got %+v", r.Findings)
	}

	mustUpdate(t, dir)
	if _, err := os.Stat(filepath.Join(dir, "db", "query.sql.go")); err != nil {
		t.Fatal("update must not delete a file it does not generate")
	}
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// nomadJobspecFunctions is the HCL2 function table of the pinned Nomad 2.0.7
// jobspec parser (jobspec2.Functions, including md5/sha1 outside FIPS mode).
// HCL rejects any other call, even in a branch a variable value never selects.
var nomadJobspecFunctions = []string{
	"abs", "abspath", "base64decode", "base64encode", "basename", "bcrypt", "can", "ceil",
	"chomp", "chunklist", "cidrhost", "cidrnetmask", "cidrsubnet", "cidrsubnets", "coalesce",
	"coalescelist", "compact", "concat", "contains", "convert", "csvdecode", "dirname", "distinct",
	"element", "file", "filebase64", "fileexists", "fileset", "flatten", "floor", "format",
	"formatdate", "formatlist", "indent", "index", "join", "jsondecode", "jsonencode", "keys",
	"length", "log", "lookup", "lower", "max", "md5", "merge", "min", "parseint", "pathexpand",
	"pow", "range", "regex_replace", "replace", "reverse", "rsadecrypt", "setintersection",
	"setproduct", "setunion", "sha1", "sha256", "sha512", "signum", "slice", "sort", "split",
	"strlen", "strrev", "substr", "timeadd", "title", "trim", "trimprefix", "trimspace",
	"trimsuffix", "try", "upper", "urlencode", "uuidv4", "uuidv5", "values", "yamldecode",
	"yamlencode", "zipmap",
}

// hclFunctionCalls returns the function names called in HCL native syntax,
// including calls inside ${...} and %{...} templates of quoted strings and
// heredocs. Comments and literal template text are skipped, as are type
// constructors such as list(string) in a variable block's type attribute,
// which are type constraints rather than function calls.
func hclFunctionCalls(src string) []string {
	s := hclScanner{src: src}
	s.code(false)
	return s.calls
}

type hclScanner struct {
	src   string
	i     int
	calls []string
}

var hclTypeConstructors = []string{"list", "map", "set", "object", "tuple", "optional"}

// code scans expressions until EOF or, inside a template interpolation, the
// closing brace that ends it. It tracks the enclosing block types and the
// current attribute so that only variable type constraints are exempt.
func (s *hclScanner) code(interpolation bool) {
	var blocks []string // innermost last: block type, or "" for an object literal
	head, assigned, typeAttr, nesting := "", false, false, 0
	for s.i < len(s.src) {
		c := s.src[s.i]
		next := byte(0)
		if s.i+1 < len(s.src) {
			next = s.src[s.i+1]
		}
		switch {
		case c == '#' || c == '/' && next == '/':
			s.skipLine()
		case c == '/' && next == '*':
			s.skipBlockComment()
		case c == '\n':
			if nesting == 0 {
				head, assigned, typeAttr = "", false, false
			}
			s.i++
		case c == '"':
			s.i++
			s.quoted()
		case c == '<' && next == '<':
			s.heredoc()
		case c == '(' || c == '[':
			nesting++
			s.i++
		case c == ')' || c == ']':
			nesting--
			s.i++
		case c == '{':
			if !assigned && head != "" && nesting == 0 {
				blocks = append(blocks, head)
				typeAttr = false
			} else {
				// Object literal: attributes inside a type constraint stay exempt.
				blocks = append(blocks, "")
			}
			head, assigned = "", false
			s.i++
		case c == '}':
			s.i++
			if len(blocks) == 0 {
				if interpolation {
					return
				}
				continue
			}
			blocks = blocks[:len(blocks)-1]
		case (c == '=' || c == '!' || c == '<' || c == '>') && next == '=', c == '=' && next == '>':
			s.i += 2
		case c == '=':
			assigned = true
			typeAttr = typeAttr || head == "type" && len(blocks) > 0 && blocks[len(blocks)-1] == "variable"
			s.i++
		case hclIdentStart(s.src[s.i:]):
			start := s.i
			for s.i < len(s.src) {
				r, size := utf8.DecodeRuneInString(s.src[s.i:])
				if r != '_' && r != '-' && !unicode.In(r, unicode.L, unicode.Mn, unicode.Mc, unicode.Nd, unicode.Pc) {
					break
				}
				s.i += size
			}
			name := s.src[start:s.i]
			if head == "" && !assigned {
				head = name
			}
			if s.callFollows() && !(typeAttr && slices.Contains(hclTypeConstructors, name)) {
				s.calls = append(s.calls, name)
			}
		default:
			s.i++
		}
	}
}

// callFollows reports whether "(" follows the cursor, allowing spaces, tabs
// and inline comments in between.
func (s *hclScanner) callFollows() bool {
	for j := s.i; j < len(s.src); {
		switch {
		case s.src[j] == ' ' || s.src[j] == '\t':
			j++
		case strings.HasPrefix(s.src[j:], "/*"):
			end := strings.Index(s.src[j+2:], "*/")
			if end < 0 {
				return false
			}
			j += end + 4
		default:
			return s.src[j] == '('
		}
	}
	return false
}

// skipLine skips a line comment up to, not including, its newline.
func (s *hclScanner) skipLine() {
	if end := strings.IndexByte(s.src[s.i:], '\n'); end >= 0 {
		s.i += end
	} else {
		s.i = len(s.src)
	}
}

func (s *hclScanner) skipBlockComment() {
	if end := strings.Index(s.src[s.i+2:], "*/"); end >= 0 {
		s.i += end + 4
	} else {
		s.i = len(s.src)
	}
}

// template handles a "$${", "%%{", "${" or "%{" sequence at the cursor and
// reports whether it consumed one.
func (s *hclScanner) template() bool {
	rest := s.src[s.i:]
	switch {
	case strings.HasPrefix(rest, "$${") || strings.HasPrefix(rest, "%%{"):
		s.i += 3
	case strings.HasPrefix(rest, "${") || strings.HasPrefix(rest, "%{"):
		s.i += 2
		s.code(true)
	default:
		return false
	}
	return true
}

func (s *hclScanner) quoted() {
	for s.i < len(s.src) {
		switch {
		case s.src[s.i] == '\\':
			s.i += 2
		case s.src[s.i] == '"':
			s.i++
			return
		case !s.template():
			s.i++
		}
	}
}

func (s *hclScanner) heredoc() {
	s.i += 2
	if s.i < len(s.src) && s.src[s.i] == '-' {
		s.i++
	}
	end := strings.IndexByte(s.src[s.i:], '\n')
	if end < 0 {
		s.i = len(s.src)
		return
	}
	marker := strings.TrimSpace(s.src[s.i : s.i+end])
	s.i += end + 1
	lineStart := true
	for s.i < len(s.src) {
		if lineStart {
			lineEnd := strings.IndexByte(s.src[s.i:], '\n')
			if lineEnd < 0 {
				lineEnd = len(s.src) - s.i
			}
			if strings.TrimSpace(s.src[s.i:s.i+lineEnd]) == marker {
				s.i += lineEnd
				return
			}
			lineStart = false
		}
		switch {
		case s.src[s.i] == '\n':
			s.i++
			lineStart = true
		case s.template():
			// An interpolation can span lines; scanning resumes after it.
		default:
			s.i++
		}
	}
}

// hclIdentStart reports whether an HCL identifier (Unicode letter or
// underscore first) starts at the beginning of src.
func hclIdentStart(src string) bool {
	r, _ := utf8.DecodeRuneInString(src)
	return r == '_' || unicode.IsLetter(r)
}

func TestHCLFunctionCallScanner(t *testing.T) {
	src := `# tostring(comment)
// lower(comment)
/* upper(comment) */
a = "literal tostring(x) $${escaped(x)} ${format("%d", var.n)} %{ if can(var.x) }y%{ endif }"
b = var.n > 0 ? join(",", [for v in var.list : trimspace(v)]) : ""
c = { nested = { d = max(1, 2) } }
data = <<EOF
shell $(not_hcl x) tostring(text) $${escaped(y)} ${lookup(var.m, "k", "")}
EOF
e = length (var.list)
variable "v" {
  type = map(object({
    a = list(string)
  }))
  default = upper("x")
}
f = abs /* inline */ (var.n)
job "j" {
  type = "service" # (
  g = floor(1)
  meta = { type = min(1, 2) }
  group "g" { type = ceil(1) }
}
h = <<EOF
${
"foo"
}${sha1("x")}
EOF
i = éformat("%d", 1)
` + "j = e\u0301format(1)\n"
	got := hclFunctionCalls(src)
	want := []string{"format", "can", "join", "trimspace", "max", "lookup", "length", "upper", "abs", "floor", "min", "ceil", "sha1", "éformat", "e\u0301format"}
	if !slices.Equal(got, want) {
		t.Fatalf("function calls = %q, want %q", got, want)
	}
}

// Nomad resolves every function call while parsing, so an unknown function
// breaks registration for all variable values, not only the branch using it.
func TestDeploymentJobspecUsesOnlyNomadFunctions(t *testing.T) {
	job := deploymentFile(t, "karakoram.nomad.hcl")
	calls := hclFunctionCalls(job)
	for _, expected := range []string{"join", "format"} {
		if !slices.Contains(calls, expected) {
			t.Fatalf("scanner missed %s() in the shipped job; calls %q", expected, calls)
		}
	}
	for _, name := range calls {
		if !slices.Contains(nomadJobspecFunctions, name) {
			t.Errorf("deploy/nomad/karakoram.nomad.hcl calls %s(), which Nomad 2.0.7 jobspecs do not provide", name)
		}
	}
}

// With a Nomad CLI (KARAKORAM_NOMAD_BIN, else nomad on PATH) this renders the
// shipped job through Nomad's own parser for zero and positive member disk
// floors, and syntax-checks the volume and ACL specifications. Rendering is
// offline; no Nomad agent or credential is used.
func TestDeploymentFilesParseWithNomadCLI(t *testing.T) {
	bin := os.Getenv("KARAKORAM_NOMAD_BIN")
	if bin == "" {
		var err error
		if bin, err = exec.LookPath("nomad"); err != nil {
			t.Skip("set KARAKORAM_NOMAD_BIN or put the nomad CLI on PATH to parse deploy/nomad with Nomad")
		}
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "NOMAD_ADDR=http://127.0.0.1:9"}
	run := func(args ...string) []byte {
		t.Helper()
		cmd := exec.Command(bin, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("nomad %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return out
	}
	version := exec.Command(bin, "version")
	version.Env = env
	if out, err := version.Output(); err == nil {
		t.Logf("using %s", strings.SplitN(string(out), "\n", 2)[0])
	}
	dir := filepath.Join("..", "..", "..", "deploy", "nomad")
	for _, tc := range []struct{ value, want string }{{"0", ""}, {"4096", "4096"}} {
		out := run("job", "run", "-output",
			"-var", "artifact_url=https://artifacts.example.invalid/karakoram.tar.gz",
			"-var", "artifact_checksum=sha256:"+strings.Repeat("0", 64),
			"-var", "nomad_address=https://nomad.example.invalid:4646",
			"-var", "public_url=https://karakoram.example.invalid",
			"-var", "member_ephemeral_disk_mb="+tc.value,
			filepath.Join(dir, "karakoram.nomad.hcl"))
		var rendered struct {
			Job struct {
				TaskGroups []struct {
					Tasks []struct {
						Name string
						Env  map[string]string
					}
				}
			}
		}
		if err := json.Unmarshal(out, &rendered); err != nil {
			t.Fatalf("member_ephemeral_disk_mb=%s: render output is not JSON: %v", tc.value, err)
		}
		found := false
		for _, group := range rendered.Job.TaskGroups {
			for _, task := range group.Tasks {
				if task.Name != "server" {
					continue
				}
				found = true
				if got, ok := task.Env["KARAKORAM_NOMAD_EPHEMERAL_DISK_MB"]; !ok || got != tc.want {
					t.Fatalf("member_ephemeral_disk_mb=%s: KARAKORAM_NOMAD_EPHEMERAL_DISK_MB = %q (present %v), want %q", tc.value, got, ok, tc.want)
				}
			}
		}
		if !found {
			t.Fatalf("member_ephemeral_disk_mb=%s: rendered job has no server task", tc.value)
		}
	}
	for _, name := range []string{"karakoram-volume.hcl", "karakoram-policy.hcl"} {
		run("fmt", "-write=false", "-list=false", filepath.Join(dir, name))
	}
}

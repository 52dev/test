package tpl

import "testing"

func TestRenderTemplates(t *testing.T) {
	dataMap := map[string]any{
		"Name":  "Alice",
		"Age":   28,
		"Color": "blue",
		"Hobby": "reading",
		"Key1":  "value1",
		"Key2":  123.45,
	}

	tests := []struct {
		name  string
		value []string
	}{
		{
			name: "Test Template 1",
			value: []string{
				"Hello, {{.Name}}! You are {{.Age}} years old.",
				"Your favorite color is {{if .Colorc}}{{.Colorc}}{{else}}未找到Color值{{end}}, and you like {{.Hobby}}.",
				"Simple template: {{.Key1}} - {{.Key2}}",
			},
		},
		{
			name: "Test Template 2",
			value: []string{
				"Hello, {{.Name}}! You are {{.Age}} years old.",
				"Your favorite color is {{if .Colorc}}{{.Colorc}}{{else}}未找到Color值{{end}}, and you like {{.Hobby}}.",
				"Simple template: {{.Key1}} - {{.Key2}}",
			},
		},
		{
			name: "Test Template 3",
			value: []string{
				"Hello, a! You are b years old.",
				"Your favorite color is a, and you like c.",
				"Simple template: c - z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := RenderTemplates(tt.value, dataMap)
			if err != nil {
				t.Errorf("RenderTemplate() error = %v", err)
				return
			}
			t.Logf("Rendered Template: %s", result)
		})
	}
}

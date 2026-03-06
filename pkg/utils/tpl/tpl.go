package tpl

import (
	"bytes"
	"fmt"
	"text/template"
)

// RenderTemplates 将模板列表与数据源合并，返回渲染后的字符串列表
// templates: 模板字符串列表
// data: 渲染模板所需的数据源
// 返回值: 渲染后的字符串列表，以及可能的错误
func RenderTemplates(templates []string, data map[string]string) ([]string, error) {
	result := make([]string, 0, len(templates))
	for idx, tplStr := range templates {
		tpl, err := template.New(fmt.Sprintf("template-%d", idx)).Parse(tplStr)
		if err != nil {
			return nil, fmt.Errorf("解析第 %d 个模板失败: %w", idx, err)
		}

		var buf bytes.Buffer
		if err := tpl.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("渲染第 %d 个模板失败: %w", idx, err)
		}

		result = append(result, buf.String())
	}
	return result, nil
}

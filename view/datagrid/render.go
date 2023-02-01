// https://gridjs.io/docs/index  go html template 渲染图表

package datagrid

import (
	"github.com/gobuffalo/packr"
	"html/template"
	"reflect"
)

type GridData struct {
	Columns []string
	Data    [][]string
}

// struct数组转换成表格,渲染出html
func Render(records []interface{}, folderPath, htmlPath string) (*template.Template, []string, [][]string) {
	box := packr.NewBox(folderPath)
	html, _ := box.FindString(htmlPath)
	tmpl, _ := template.New("datagrid").Parse(html)
	//tmpl := template.Must(template.ParseFiles(box.Path))
	var columns []string
	var row [][]string
	if len(records) > 0 {
		// 根据struct解析column数据
		t := reflect.TypeOf(records[0])
		for k := 0; k < t.NumField(); k++ {
			fileName := t.Field(k).Name
			columns = append(columns, fileName)
		}
		// 解析表格数据
		for _, record := range records {
			var rowField []string
			for _, column := range columns {
				vv := reflect.ValueOf(record)
				v := vv.FieldByName(column).String()
				rowField = append(rowField, v)
			}
			row = append(row, rowField)
		}
	}
	return tmpl, columns, row
}

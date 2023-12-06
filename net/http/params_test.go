package http

import (
	"net/url"
	"testing"
)

func TestValues2Struct(t *testing.T) {
	type args struct {
		values url.Values
		s      interface{}
	}

	want1 := url.Values{}
	want1.Set("name", "hds")
	want1.Set("age", "30")
	want1.Set("sex", "true")
	want1.Set("money", "12.344")

	s := struct {
		Name  string  `param:"name"`
		Age   int     `param:"age"`
		Sex   bool    `param:"sex"`
		Money float32 `param:"money"`
	}{Name: "hds", Age: 30, Sex: true, Money: 12.344}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test base type",
			args: args{
				values: want1,
				s:      s,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Values2Struct(tt.args.values, tt.args.s); (err != nil) != tt.wantErr {
				t.Errorf("Values2Struct() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

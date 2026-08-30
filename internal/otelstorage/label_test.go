package otelstorage

import "testing"

func TestUnescapeLabelName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no_prefix",
			input: "normal_label",
			want:  "normal_label",
		},
		{
			name:  "decode_dot",
			input: "U__k8s_2e_node_2e_name",
			want:  "k8s.node.name",
		},
		{
			name:  "decode_dash",
			input: "U__my_2d_label",
			want:  "my-label",
		},
		{
			name:  "decode_slash",
			input: "U__path_2f_to_2f_resource",
			want:  "path/to/resource",
		},
		{
			name:  "mixed_encodings",
			input: "U__k8s_2e_io_2f_app_2d_name",
			want:  "k8s.io/app-name",
		},
		{
			name:  "underscore_not_encoded",
			input: "U__some_label_name",
			want:  "some_label_name",
		},
		{
			name:  "partial_encoding",
			input: "U__test_2x_value",
			want:  "test_2x_value",
		},
		{
			name:  "empty_after_prefix",
			input: "U__",
			want:  "",
		},
		{
			name:  "only_dots",
			input: "U___2e__2e__2e_",
			want:  "...",
		},
		{
			name:  "trailing_underscore_number",
			input: "U__label_2",
			want:  "label_2",
		},
		{
			name:  "incomplete_encoding_at_end",
			input: "U__label_2e",
			want:  "label_2e",
		},
		{
			name:  "decode_colon",
			input: "U__service_3a_name",
			want:  "service:name",
		},
		{
			name:  "decode_space",
			input: "U__hello_20_world",
			want:  "hello world",
		},
		{
			name:  "decode_at_sign",
			input: "U__user_40_domain",
			want:  "user@domain",
		},
		{
			name:  "decode_uppercase_hex",
			input: "U__test_2E_value",
			want:  "test.value",
		},
		{
			name:  "invalid_hex_single_digit",
			input: "U__test_2_value",
			want:  "test_2_value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UnescapeLabelName(tt.input)
			if got != tt.want {
				t.Errorf("UnescapeLabelName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

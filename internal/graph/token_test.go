package graph

import (
	"reflect"
	"testing"
)

func TestConfigValueTokens(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "plain name", value: "invoices.created", want: []string{"invoices.created"}},
		{
			name: "mounted application.yml",
			value: "spring:\n  kafka:\n    bootstrap-servers: kafka-1:9092,kafka-2:9092\n" +
				"    consumer:\n      group-id: \"billing\"\n" +
				"app:\n  topics: [orders.created, payments_v2]\n  out: 'invoices.created'\n",
			want: []string{
				"spring", "kafka", "bootstrap-servers", "kafka-1", "9092", "kafka-2", "consumer", "group-id", "billing",
				"app", "topics", "orders.created", "payments_v2", "out", "invoices.created",
			},
		},
		{name: "properties", value: "app.topic=orders.created;app.dlq=orders.dlq", want: []string{"app.topic", "orders.created", "app.dlq", "orders.dlq"}},
		{name: "json", value: `{"topic":"orders.created"}`, want: []string{"topic", "orders.created"}},
		{name: "empty", value: " \n\t", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ConfigValue{Value: test.value}.Tokens()
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("tokens = %q, want %q", got, test.want)
			}
		})
	}
}

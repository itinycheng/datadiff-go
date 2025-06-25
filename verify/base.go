package verify



// pk = order by + partition
type DataPool struct {
	source map[string]any
	target map[string]any
}

func (data *DataPool) Verify() {

}

func queryVerifyTables(database string) ([]string, error) {
	// This function should query the tables from the database and return them in a map.
	// The implementation is omitted for brevity.
	return nil, nil
}

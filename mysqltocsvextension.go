// mysqltocsvextension is a package that extends
// to make it dead easy to turn arbitrary database query
// results (in the form of database/sql Rows) into CSV output.
//
// Source and README at https://github.com/joho/sqltocsv
package sqltocsv

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

// WriteWithDelim ...
func (c Converter) WriteWithDelim(writer io.Writer, delim rune) error {
	rows := c.rows
	csvWriter := csv.NewWriter(writer)
	csvWriter.Comma = '|'

	columnNames, err := rows.Columns()
	if err != nil {
		return err
	}

	if c.WriteHeaders {
		// use Headers if set, otherwise default to
		// query Columns
		var headers []string
		if len(c.Headers) > 0 {
			headers = c.Headers
		} else {
			headers = columnNames
		}
		err = csvWriter.Write(headers)
		if err != nil {
			// TODO wrap err to say it was an issue with headers?
			return err
		}
	}

	count := len(columnNames)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		row := make([]string, count)

		for i, _ := range columnNames {
			valuePtrs[i] = &values[i]
		}

		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}

		for i, _ := range columnNames {
			var value interface{}
			rawValue := values[i]

			byteArray, ok := rawValue.([]byte)
			if ok {
				value = string(byteArray)
			} else {
				value = rawValue
			}

			timeValue, ok := value.(time.Time)
			if ok && c.TimeFormat != "" {
				value = timeValue.Format(c.TimeFormat)
			}

			if value == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", value)
			}
		}

		writeRow := true
		if c.rowPreProcessor != nil {
			writeRow, row = c.rowPreProcessor(row, columnNames)
		}
		if writeRow {
			err = csvWriter.Write(row)
			if err != nil {
				// TODO wrap this err to give context as to why it failed?
				return err
			}
		}
	}
	err = rows.Err()

	csvWriter.Flush()

	return err
}

func createCsvWriters(writers map[int]*os.File) map[int]*csv.Writer {

	csvWritersPtrs := make(map[int]*csv.Writer)

	for k, w := range writers {
		csvW := csv.NewWriter(w)
		csvW.Comma = '|'
		csvWritersPtrs[k] = csvW
	}
	return csvWritersPtrs
}

// WriteWithDelimbyYear ...
func (c Converter) WriteWithDelimbyYear(writers map[int]*os.File, delim rune, prefix string) error {
	rows := c.rows

	csvWriters := createCsvWriters(writers)
	for _, w := range csvWriters {
		w.Comma = '|'
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return err
	}

	if c.WriteHeaders {
		// use Headers if set, otherwise default to
		// query Columns
		var headers []string
		if len(c.Headers) > 0 {
			headers = c.Headers
		} else {
			headers = columnNames
		}
		for _, w := range csvWriters {
			err := w.Write(headers)
			if err != nil {
				// TODO wrap err to say it was an issue with headers?
				return err
			}
		}

	}

	count := len(columnNames)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		row := make([]string, count)

		for i, _ := range columnNames {
			valuePtrs[i] = &values[i]
		}

		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}

		for i, _ := range columnNames {
			var value interface{}
			rawValue := values[i]

			byteArray, ok := rawValue.([]byte)
			if ok {
				value = string(byteArray)
			} else {
				value = rawValue
			}

			timeValue, ok := value.(time.Time)
			if ok && c.TimeFormat != "" {
				value = timeValue.Format(c.TimeFormat)
			}

			if value == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", value)
			}
		}

		writeRow := true
		if c.rowPreProcessor != nil {
			writeRow, row = c.rowPreProcessor(row, columnNames)
		}

		if writeRow {

			year := getYear(row)
			if csvWriters[year] == nil {

				csvW, err := createCsvFile(prefix, year)
				if err != nil {
					return err
				}

				var headers []string
				if len(c.Headers) > 0 {
					headers = c.Headers
				} else {
					headers = columnNames
				}
				csvW.Write(headers)
				csvWriters[year] = csvW

			}

			csvWriter := csvWriters[year]
			err = csvWriter.Write(row)
			if err != nil {
				// TODO wrap this err to give context as to why it failed?
				return err
			}
		}
	}
	err = rows.Err()

	for _, w := range csvWriters {
		w.Flush()
	}

	return err
}

func getYear(r []string) int {
	sDtFin := r[0]
	sYear := sDtFin[0:4]
	year, err := strconv.Atoi(sYear)
	if err != nil {
		year = 0
	}
	return year
}

func createCsvFile(prefix string, year int) (*csv.Writer, error) {

	file, err := os.Create(prefix + "_ano_" + strconv.Itoa(year) + ".csv")
	if err != nil {
		return nil, err
	}
	csvW := csv.NewWriter(file)
	csvW.Comma = '|'
	return csvW, nil
}

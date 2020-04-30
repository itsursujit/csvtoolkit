package csvtk

import (
	"bufio"
	"fmt"
	"io"
	"os"
	//"runtime"

	"gopkg.in/cheggaaa/pb.v2"
)

type InputFlags struct {
	PrintFreq int
	Buffsize  int
	Lines     bool
	Total     int
	Files     []string
}

//Output ...
func Output(in InputFlags, outFile string) error {

	//config := getConfigs(cmd)//todo:sachin:might want to add configs
	//outFile := config.OutFile
	files := in.Files
	flagLines := in.Lines
	flagBuff := in.Buffsize
	flagFreq := in.PrintFreq
	flagTotal := in.Total

	of := os.Stdout
	defer of.Close()
	if outFile != "-" {
		var err error
		of, err = os.Create(outFile)
		if err != nil {
			return err
		}
	}
	writer := bufio.NewWriterSize(of, flagBuff)
	defer writer.Flush()

	for _, file := range files {
		fmt.Fprintf(os.Stderr, "Streaming file: %s\n", file)
		fh := os.Stdin
		if file != "-" {
			var err error
			fh, err = os.Open(file)
			if err != nil {
				return err
			}
		}
		reader := bufio.NewReaderSize(fh, flagBuff)
		var bar *pb.ProgressBar

		if flagLines {
			if flagTotal < 0 {
				fmt.Fprintf(os.Stderr, "Cannot read lines unless the of expected number of lines is specified via -s!\n")
				os.Exit(1)
			}
			bar = pb.StartNew(flagTotal)
			var line []byte
			var err error
			var count int
			for {
				line, err = reader.ReadBytes('\n')
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				count++
				if count%flagFreq == 0 {
					bar.Add(1)
				}
				_, err := writer.Write(line)
				if err != nil {
					return err
				}
			}

		} else {
			var err error
			if flagTotal < 0 {
				if file == "-" {
					fmt.Fprintf(os.Stderr, "Cannot read from stdin unless the number of expected bytes is specified via -s!\n")
					os.Exit(1)
				}
				inputStat, err := os.Stat(file)
				if err != nil {
					return err
				}
				flagTotal = int(inputStat.Size())

			}
			bar = pb.StartNew(flagTotal)
			byteBuff := make([]byte, flagBuff)
			var count int
			var bytesSince int
			var readSize int
			for {
				readSize, err = reader.Read(byteBuff)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				count++
				bytesSince += readSize
				if count%flagFreq == 0 {
					bar.Add(bytesSince)
					bytesSince = 0
				}
				_, err = writer.Write(byteBuff[:readSize])
				if err != nil {
					return err
				}
			}

		}

		bar.Finish()
		fh.Close()

	}
	return nil
}

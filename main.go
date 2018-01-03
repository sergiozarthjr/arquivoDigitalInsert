package main

import(
	"os"
	"fmt"
	"strings"
	"path/filepath"
	_ "database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"flag"
	"database/sql"
	"log"
	"time"
	"strconv"
)
var (
	password = flag.String("password", "*", "the database password")
	port *int = flag.Int("port", 1433, "the database port")
	server = flag.String("server", "*", "the database server")
	user = flag.String("user", "*", "the database user")
	database = flag.String("database", "*", "the database")
	query = "INSERT INTO [dbo].[Arquivo] (CodigoGeo, DataEvento, LinkDownload, NomeArquivo, NumeroCobrade, SiglaEstado, TipoDoc) values (%s, '%s', '%s', '%s', '%s', '%s', '%s')"
	linkftp = "*"
	searchDir = "*"
)

func main() {
	flag.Parse()

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", *server, *user, *password, *port, *database)
	conn, err := sql.Open("mssql", connString)

	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}

	var fileList []string
	filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		fileList = append(fileList, f.Name())
		return nil
	})

	for _, file := range fileList {
		s := strings.Split(file, "-")
		if len(s) > 1 {
			nomeArquivo := file

			siglaEstado := s[0]
			tipoDoc := s[1]
			codigoGeo := s[2]
			numeroCobrade := s[3]

			ano, _ := strconv.Atoi(s[4][:4])
			mes, _ := strconv.Atoi(s[4][4:6])
			dia, _ := strconv.Atoi(s[4][6:8])

			dataEvento := time.Date(ano, time.Month(mes), dia,0,0,0,0, time.Local)

			linkDownload := linkftp + nomeArquivo

			sql := fmt.Sprintf(query, codigoGeo, dataEvento.Format("2006-01-02 15:04:05"), linkDownload, nomeArquivo, numeroCobrade, siglaEstado, tipoDoc)

			_, err = conn.Exec(sql)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	}
	defer conn.Close()
}

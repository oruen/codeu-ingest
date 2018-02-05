Package client:

    sbt universal:packageZipTarball

Run client:

    ACCESS_TOKEN=123 MAX=50 PORT=8080 ./codeu-upload-1.0.0/bin/codeu-upload

Run test server:

    cd src/server
    go run main.go

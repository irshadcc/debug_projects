
function build() {
    mvn clean package ;
}

function run() {

    CLASS_PATH=$(realpath target/arrow-bug-demo-1.0-SNAPSHOT.jar);
    java -cp ${CLASS_PATH} \
        --add-opens=java.base/java.nio=ALL-UNNAMED \
        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        com.example.demo.BugDemo ;
}




case $1 in
    "build") build ;;
    "run") run ;;
esac

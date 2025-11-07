export BUCKET=todo

mvn clean package -DskipTests -Dduckdb.version=1.2.2.0
cp target/duck_secrets-0.0.1-SNAPSHOT.jar debug_secrets_122.jar
aws s3 cp debug_secrets_122.jar "s3://${BUCKET}/debug/debug_secrets_122.jar"

mvn clean package -DskipTests -Dduckdb.version=1.3.0.0
cp target/duck_secrets-0.0.1-SNAPSHOT.jar debug_secrets_130.jar
aws s3 cp debug_secrets_130.jar "s3://${BUCKET}/debug/debug_secrets_130.jar"


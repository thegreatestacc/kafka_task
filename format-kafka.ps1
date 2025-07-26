# Configuration
$clusterId = "xe2d5Se9STyfJOLtJGruMQ"
$image = "confluentinc/cp-kafka:7.5.0"

Write-Host "Formatting Kafka brokers with KRaft Cluster ID: $clusterId"

1..3 | ForEach-Object {
    $id = $_
    $brokerName = "kafka-$id"
    $dataPath = "${PWD}\kafka-cluster\$brokerName\data"
    $confPath = "${PWD}\kafka-cluster\$brokerName\config"
    $confFile = "$confPath\server.properties"

    # Ensure folders exist
    New-Item -ItemType Directory -Force -Path $dataPath | Out-Null
    New-Item -ItemType Directory -Force -Path $confPath | Out-Null

    # Clean data directory
    Remove-Item -Recurse -Force -ErrorAction Ignore "$dataPath\*"

    # Create valid server.properties
    $serverProps = @(
        "node.id=$id"
        "process.roles=broker,controller"
        "controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
        "controller.listener.names=CONTROLLER"
        "listeners=PLAINTEXT://${brokerName}:9092,CONTROLLER://${brokerName}:9093"
        "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
        "log.dirs=/var/lib/kafka/data"
    )
    $utf8NoBomEncoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines($confFile, $serverProps, $utf8NoBomEncoding)

    # Debug info
    Write-Host "`nid: $id"
    Write-Host "brokerName: $brokerName"
    Write-Host "PWD: $PWD"
    Write-Host "confPath: $confPath"

    $dockerDataPath = $dataPath -replace '\\','/'
    $dockerConfPath = $confPath -replace '\\','/'

    Write-Host ""
    Write-Host "Formatting $brokerName..."
    Write-Host "dockerDataPath: $dockerDataPath"
    Write-Host "dockerConfPath: $dockerConfPath"

    docker run --rm `
        -v "${dockerDataPath}:/var/lib/kafka/data" `
        -v "${dockerConfPath}:/etc/kafka" `
        $image `
        kafka-storage format --config /etc/kafka/server.properties --cluster-id $clusterId --ignore-formatted

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to format $brokerName"
        exit 1
    }

    $metaPath = "$dataPath\meta.properties"
    if (Test-Path $metaPath) {
        Write-Host "$brokerName formatted successfully."
    } else {
        Write-Host "${brokerName}: meta.properties not found. Something went wrong."
    }
}

Write-Host ""
Write-Host "All brokers formatted. You can now run: docker-compose up -d"

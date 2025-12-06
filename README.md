## Установка
```
git clone https://github.com/dozenheroic/cluster.YP.git
cd cluster.YP
```
## Сборка
```
go build -o bin/payload ./cmd/payload
go build -o bin/agent ./cmd/agent
go build -o bin/controller ./cmd/controller
```
## BM1 Controller
```
set CONTROLLER_PORT=8080     
set DESIRED_REPLICAS=3

.\bin\controller.exe
```
## BM2 Agent1
```
set AGENT_PORT=9000
set AGENT_URL=http://10.0.0.2:9000
set CONTROLLER_URL=http://10.0.0.1:8080

.\bin\agent.exe
```
## BM3 Agent2
```
set AGENT_PORT=9000
set AGENT_URL=http://10.0.0.3:9000
set CONTROLLER_URL=http://10.0.0.1:8080

.\bin\agent.exe
```
## Проверка статуса
```
curl http://10.0.0.1:8080/status
```
Статус агента
```
curl http://10.0.0.2:9000/status
```

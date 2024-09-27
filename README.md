# flink-

## flink学习

### 版本
- 1.16.1

### 内容
- 1、基础datastream api学习
- 2、状态后端与状态恢复
- 3、flink table API

### flink on k8s

提前条件,在k8s集群中做下面的操作
```xml

1、kubectl create namespace ns-ybx
2、kubectl create serviceaccount sa-ybx -n ns-ybx
# 创建服务账号
3、kubectl describe serviceaccount -n ns-ybx
# 创建获取镜像的secrets
4、kubectl patch serviceaccount sa-ybx -p '{"imagePullSecrets": [{"name": "atfm-test"}]}' -n ns-ybx

5、kubectl create clusterrolebinding flink-role-binding-flink \
  --clusterrole=edit \
  --serviceaccount=ns-ybx:sa-ybx
```

- 1、application 模式

```shell
./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.namespace=ns-ybx \
    -Dkubernetes.service-account=sa-ybx \
    -Dkubernetes.cluster-id=test-flink-1 \
    -Dkubernetes.container.image=registry.chongqing.zxyh.tcs230.fsphere.cn/library/flink_1.16.2_test:1.16.2 \
    -Dkubernetes.taskmanager.cpu=1 \
    -Dkubernetes.jobmanager.cpu=1 \
    -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=1024m \
    -Djobkmanager.memory.process.size=1024m \
    local:///opt/flink/examples/streaming/SocketWindowWordCount.jar --hostname 10.1.0.18 --port 9195 \
    --windowSize 2000 --slideSize 10000
```

- 2、session 模式


```shell
-- 先启动session
./bin/kubernetes-session.sh \
-Dkubernetes.namespace=ns-ybx \
-Dkubernetes.service-account=sa-ybx \
-Dkubernetes.cluster-id=test-flink-1 \
-Dkubernetes.container.image=registry.chongqing.zxyh.tcs230.fsphere.cn/library/flink_1.16.2_test:1.16.2 \
-Dkubernetes.taskmanager.cpu=1 \
-Dkubernetes.jobmanager.cpu=1 \
-Dtaskmanager.numberOfTaskSlots=1 \
-Dtaskmanager.memory.process.size=1024m \
-Djobkmanager.memory.process.size=1024m 

-- session模式提交作业
./bin/flink run -d \
-e kubernetes-session \
-Dkubernetes.namespace=ns-ybx \
-Dkubernetes.cluster-id=test-flink-1 \
/opt/flink/examples/streaming/SocketWindowWordCount.jar --hostname 10.1.0.18 --port 9195 \
--windowSize 2000 --slideSize 10000
```



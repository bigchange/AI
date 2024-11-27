# [Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/)

一种CRD资源，方便管理运行Spark程序所需要的资源配置，包括：SparkConf、 Driver、Executor等。

编写好对应的SparkOperator的yaml文件，即可通过kubectl apply -f xxx.yaml来运行Spark程序，非常方便。

## 参考文档
- [Spark Operator API](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md#sparkoperator.k8s.io/v1beta2.SparkApplication)
- [Kubeflow文档](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md)
- [Yaml文件编写说明](https://www.kubeflow.org/docs/components/spark-operator/user-guide/writing-sparkapplication/)


不建议使用Kafka实现延时队列，实现复杂。
1. 修改kafka内核代码实现延时队列
2. 实现一个延迟服务，从延迟队列获取消息，并重新投递
3. 实现延迟模块
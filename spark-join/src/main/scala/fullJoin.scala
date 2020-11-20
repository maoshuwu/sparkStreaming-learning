import org.apache.spark.streaming.dstream.DStream

/**
  * Created by $maoshuwu on 2020/11/2.
  */
def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
  val orderIdAndOrderInfo: DStream[(String, OrderInfo)] =
    orderInfoStream.map(info => (info.id, info))
  val orderIdAndOrderDetail: DStream[(String, OrderDetail)] =
    orderDetailStream.map(info => (info.order_id, info))

  orderIdAndOrderInfo
    .fullOuterJoin(orderIdAndOrderDetail)
    .mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
      // 获取redis客户端
      val client: Jedis = RedisUtil.getClient
      // 读写操作
      val result: Iterator[SaleDetail] = it.flatMap {
        // order_info有数据, order_detail有数据
        case (orderId, (Some(orderInfo), Some(orderDetail))) =>
          println("Some(orderInfo)   Some(orderDetail)")
          // 1. 把order_info信息写入到缓存(因为order_detail信息有部分信息可能迟到)
          cacheOrderInfo(orderInfo, client)
          // 2. 把信息join到一起(其实就是放入一个样例类中)  (缺少用户信息, 后面再专门补充)
          val saleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          // 3. 去order_detail的缓存找数据, 进行join
          // 3.1 先获取这个order_id对应的所有的order_detail的key
          import scala.collection.JavaConversions._
          val keys: List[String] = client.keys("order_detail:" + orderInfo.id + ":*").toList // 转成scala集合
        val saleDetails: List[SaleDetail] = keys.map(key => {
          val orderDetail: OrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
          // 删除对应的key, 如果不删, 有可能造成数据重复
          client.del(key)
          SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
        })
          saleDetail :: saleDetails
        case (orderId, (Some(orderInfo), None)) =>
          println("Some(orderInfo), None")
          // 1. 把order_info信息写入到缓存(因为order_detail信息有部分信息可能迟到)
          cacheOrderInfo(orderInfo, client)
          // 3. 去order_detail的缓存找数据, 进行join
          // 3.1 先获取这个order_id对应的所有的order_detail的key
          import scala.collection.JavaConversions._
          val keys: List[String] = client.keys("order_detail:" + orderInfo.id + ":*").toList // 转成scala集合
        val saleDetails: List[SaleDetail] = keys.map(key => {
          val orderDetail: OrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
          // 删除对应的key, 如果不删, 有可能造成数据重复
          client.del(key)
          SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
        })
          saleDetails
        case (orderId, (None, Some(orderDetail))) =>
          println("None, Some(orderDetail)")
          // 1. 去order_info的缓存中查找
          val orderInfoJson = client.get("order_info:" + orderDetail.order_id)
          if (orderInfoJson == null) {
            // 3. 如果不存在, 则order_detail缓存
            cacheOrderDetail(orderDetail, client)
            Nil
          } else {
            // 2. 如果存在, 则join
            val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
          }
      }

      // 关闭redis客户端
      client.close()

      result
    })

}
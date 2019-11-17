package com.lancer.rabbitmq

import androidx.appcompat.app.AppCompatActivity
import android.os.Bundle
import android.util.Log
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import kotlinx.android.synthetic.main.activity_main.*

class MainActivity : AppCompatActivity() {

    private val connectionFactory: ConnectionFactory = ConnectionFactory()
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        initData()
    }

    private fun initData() {
        setupConnectionFactory()
        send_msg_bt.setOnClickListener {
            Thread {
                publish()
            }.start()
        }
       receiver_msg_bt.setOnClickListener {
           Thread {
               consume()
           }.start()
       }

    }

    /**
     * rabbit配置
     */
    private fun setupConnectionFactory() {
        connectionFactory.username = "guest"
        connectionFactory.password = "guest"
        connectionFactory.host = "192.168.21.107"
        connectionFactory.port = 15672
    }

    /**
     * rabbit发送消息
     */
    private fun publish() {
        //连接
        val connection = connectionFactory.newConnection()
        //通道
        val channel = connection.createChannel()
        //消息发布
        val msg = "hello girl friend!".toByteArray()
        channel.basicPublish("demo", "lancer", null, msg)
    }

    private fun consume() {
        val connection = connectionFactory.newConnection()
        val channel = connection.createChannel()
        //声明了一个交换和一个服务器命名的队列，然后将它们绑定在一起。
        channel.exchangeDeclare("demo", "fanout", true)
        val queueName = channel.queueDeclare().getQueue()
        Log.e("TAG", "$queueName :queueName")
        channel.queueBind(queueName, "demo", "lancer")
        //实现Consumer的最简单方法是将便捷类DefaultConsumer子类化。可以在basicConsume 调用上传递此子类的对象以设置订阅：
        channel.basicConsume(queueName, false, "", object : DefaultConsumer(channel) {
            override fun handleDelivery(
                consumerTag: String?,
                envelope: Envelope?,
                properties: AMQP.BasicProperties?,
                body: ByteArray?
            ) {
                val rountingKey = envelope?.getRoutingKey() //路由密钥
                val contentType = properties?.getContentType() //contentType字段，如果尚未设置字段，则为null。
                val msg = body.toString() //接收到的消息 String msg = new String(body,"UTF-8") ;
                val deliveryTag = envelope?.getDeliveryTag();//交付标记

                Log.e("TAG", "$rountingKey：rountingKey")
                Log.e("TAG", "$contentType：contentType")
                Log.e("TAG", "$msg：msg")
//                Log.e("TAG" , deliveryTag+"：deliveryTag")
                Log.e("TAG", "$consumerTag：consumerTag")
                Log.e("TAG", envelope!!.getExchange() + "：exchangeName")
                receiver_msg_tv.text = msg
                channel.basicAck(deliveryTag!!, false)

            }
        })


    }
}

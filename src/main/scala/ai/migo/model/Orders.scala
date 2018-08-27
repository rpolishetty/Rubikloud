package ai.migo.model

/*
 * created by prohith on 8/26/18
 */

case class Orders(orderKey: String,
                  custKey: String,
                  orderStatus: String,
                  totalPrice: String,
                  orderDate: String,
                  orderPriority: String,
                  clerk: String,
                  shipPriority: String,
                  comment: String
                 )

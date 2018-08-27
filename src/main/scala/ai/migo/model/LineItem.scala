package ai.migo.model

/*
 * created by prohith on 8/25/18
 */

case class LineItem(orderKey: String,
                    partKey: String,
                    suppKey: String,
                    lineNumber: String,
                    quantity: String,
                    extendedPrice: String,
                    discount: String,
                    tax: String,
                    returnFlag: String,
                    lineStatus: String,
                    shipDate: String,
                    commitDate: String,
                    receiptDate: String,
                    shipinStruct: String,
                    shipMode: String,
                    comment: String
                   )

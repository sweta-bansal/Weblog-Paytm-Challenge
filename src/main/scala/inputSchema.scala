
/*
Created a dataset for input data and processed data
 */

case class SessionData(
                             timestamp                   : String
                             ,elb                        : String
                             ,clientIPandPort                   : String
                             ,backendIPandPort                 : String
                             ,request_processing_time                : String
                             ,backend_processing_time    : String
                             ,response_processing_time    : String
                             ,elb_status_code   : String
                             ,backend_status_code           : String
                             ,received_bytes       : String
                             ,sent_bytes             : String
                             ,URL                        : String
                             ,browser                     : String
                             ,ssl_cipher               : String
                             ,ssl_protocol                : String

                           )

case class ProcessedData(
                        timestamp                   : String
                        ,clientIP                   : String
                        ,clientPort                 : String
                        ,URL                        : String
                        ,browser                    : String

                      )


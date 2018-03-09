package io.elegans.oracsdk.etl

class Load {

  /**
    * Produces a Json string with all typical fields needed for a book. Category is
    * "book" by default. Returns a tuple (id, json) because the id is needed
    * for PUTing modifications.
    *
    * @param id
    * @param name
    * @param category
    * @param author
    * @param language
    * @param pages
    * @param unixTimeStamp publication timestamp in seconds
    * @param isbn
    * @return (id, itemJson)
    */
  def makeBookJson(id:String, name:String, category:String="book",
                   author:Option[String]=None,
                   language:Option[String]=None, pages:Option[Int]=None,
                   unixTimeStamp:Option[Long]=None, isbn:Option[String]=None):(String, String) = {

    val string =  "[" + List(
      author match {
        case Some(v) => Some(s"""{"key": "author", "value": "$v"}""")
        case _ => None
      },
      language match {
        case Some(v) => Some(s"""{"key": "language", "value": "$v"}""")
        case _ => None
      },
      isbn match {
        case Some(v) => Some(s"""{"key": "isbn", "value": "$v"}""")
        case _ => None
      }
    ).filter(_ != None).map(_.getOrElse(" ")).mkString(", ") + "]"

    val numerical = "[" + List(
      pages match {
        case Some(v) => Some(s"""{"key": "pages", "value": $v}""")
        case _ => None
      }
    ).filter(_ != None).map(_.getOrElse(" ")).mkString(", ") + "]"

    val timestamp = "[" + List(
      unixTimeStamp match {
        case Some(v) => Some(s"""{"key": "publication", "value": ${v*1000} }""")
        case _ => None
      }
    ).filter(_ != None).map(_.getOrElse(" ")).mkString(", ") + "]"

    val props = "{" + List(
      string match {
        case v: String =>
          Some(s""" "string": $v """)
        case _ => None
      },
      numerical match {
        case v: String => Some(s""" "numerical": $v """)
        case _ => None
      },
      timestamp match {
        case v: String => Some(s""" "timestamp": $v """)
        case _ => None
      }
    ).filter(_ != None).map(_.getOrElse("")).mkString(", ") + "}"

    (id, s"""{"id": "$id", "name": "$name", "category": "$category", "props": $props }""")
  }

  /**
    *
    * @param id
    * @param name
    * @param creator_uid
    * @param user_id
    * @param item_id
    * @param timestamp
    * @param score
    * @param ref_url
    * @param ref_recommendation
    * @return (id or "", ActionJson)
    */
  def makeActionJson(id:Option[String]=None, name:String,
                     creator_uid:Option[String]=None,
                     user_id:String,
                     item_id:String,
                     timestamp:Option[Long]=None,
                     score:Option[Double]=None,
                     ref_url:Option[String]=None,
                     ref_recommendation:Option[String]=None):(String, String) = {

    val jsonString = "{" + s""" "name": $name, """ +
      s""" item_id": $item_id, """ +
      s""" item_id": $item_id, """ +
      List(
        id match {
          case Some(v) => Some(s""" "id": $v """)
          case _ => None
        },
        creator_uid match {
          case Some(v) => Some(s""" "creator_uid": $v """)
          case _ => None
        },
        timestamp match {
          case Some(v) => Some(s""" "timestamp": $v """)
          case _ => None
        },
        score match {
          case Some(v) => Some(s""" "score": $v """)
          case _ => None
        },
        ref_url match {
          case Some(v) => Some(s""" "ref_url": $v """)
          case _ => None
        },
        ref_recommendation match {
          case Some(v) => Some(s""" "ref_recommendation": $v """)
          case _ => None
        }
      ).filter(_ != None).map(_.getOrElse("")).mkString(", ") + "}"

    (
      id match {
        case Some(v) => v
        case _ => ""
      },
      jsonString)
  }

}

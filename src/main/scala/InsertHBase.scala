object InsertHBase {
  def main(args: Array[String]): Unit = {
    InsertMusics.insertFromMusics()
    InsertUsers.insertFromUsers()
    InsertSingers.insertFromSingers()
    InsertUser_Music.insertFromUser_Music()
    InsertUser_Singer.insertFromUser_Singer()
    InsertUser_Singer.insertFromRating()
  }
}

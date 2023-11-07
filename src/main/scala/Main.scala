import java.nio.file.Paths

object Main extends App {

  val clicksDir = Paths.get("data", "clicks")
  val usersDir = Paths.get("data", "users")

  val task1 = new MapReduce[String, Map[String, String]](
    8,
    Ordering.String,
    new CSVDataManager((key, values) => values.map(value => key + "," + value.values.mkString(",")))
  )

  task1(
    Map(clicksDir -> (lines => lines.map(line => (line("date"), line)))),
    (key, values) => Seq(Map("total_clicks" -> values.length.toString)),
    Paths.get("data", "total_clicks")
  )

  val task2 = new MapReduce[String, Map[String, String]](
    8,
    Ordering.String,
    new CSVDataManager((key, values) => values.map(value => key + "," + value.values.mkString(",")))
  )

  task2(
    Map(
      usersDir -> (
        _.filter(_("country") == "LT")
        .map(line => (line("id"), line ++ Map("table" -> "users")))
      ),
      clicksDir -> (_.map(line => (line("user_id"), line ++ Map("table" -> "clicks"))))
    ),
    (key, values) => {
      val user = values.find(_("table") == "users").getOrElse(Map.empty)

      values
        .filter(_("table") == "clicks")
        .map(click => click ++ user)
        .filter(_.contains("country"))
    },
    Paths.get("data", "filtered_clicks")
  )
}
// Dashboard Utilities
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


object DateUtil {
/** Date utility for manipulating/generating dashboard components as required
**/
  private val dateFmt = "yyyy-MM-dd"

  def today(): String = {
    val date = new Date
    val sdf = new SimpleDateFormat(dateFmt)
    sdf.format(date)
  }

  def yesterday(): String = {
    val calender = Calendar.getInstance()
    calender.roll(Calendar.DAY_OF_YEAR, -1)
    val sdf = new SimpleDateFormat(dateFmt)
    sdf.format(calender.getTime())
  }

  def daysAgo(days: Int): String = {
    val calender = Calendar.getInstance()
    calender.roll(Calendar.DAY_OF_YEAR, -days)
    val sdf = new SimpleDateFormat(dateFmt)
    sdf.format(calender.getTime())
  }

} 

def categorizeResult(x: Float): Unit = x match {
    case diffMoreThan100 if(diffMoreThan100 > 100) =>  println("%html <h1 style='color:green;font-size:75px;text-align: center;'> " + "%02.2f".format(diffMoreThan100.toFloat)  + "%</h1>")
    case diffLessThan101 if (diffLessThan101 <= 100 && diffLessThan101 > -5) =>  println("%html <h1 style='color:blue;font-size:75px;text-align: center;'> " +"%02.2f".format(diffLessThan101.toFloat) + "%</h1>")
    case diffLessThanNeg5 if diffLessThanNeg5 < -5 =>  println("%html <h1 style='color:red;font-size:75px;text-align: center;'> " + "%02.2f".format(diffLessThanNeg5.toFloat)  + "%</h1>")
    case _ => println("%html <h1 style='color:pink;font-size:55px;text-decoration: underline; text-align: center;'> Alert!</h1>")
}

// Extract column data changes
def extractFeatureCountDifference(dataSource: String,
                                  columnName: String,
                                  currentDate: String = DateUtil.today,
                                  referenceDate: String = DateUtil.yesterday): Float = {
    println("%html <h5 style='color:Tomato;font-size:13px; text-align: center; text-transform: uppercase; letter-spacing: 1px;  '>Processed dates: " + currentDate + " VS " + referenceDate + "</h5>")  
    var countRowsToday = spark.sql(s"select ${columnName} from ${dataSource} where key_date='${currentDate}'").collect
    var countRowsYesterday = spark.sql(s"select  ${columnName} from ${dataSource} where key_date='${referenceDate}'").collect

    var diffPercentage = (((countRowsToday(0)(0) + "").toFloat - (countRowsYesterday(0)(0) + "").toFloat) / (countRowsYesterday(0)(0) + "").toFloat) * 100
    
    diffPercentage
  }

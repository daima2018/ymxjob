import org.joda.time.DateTime

import java.text.SimpleDateFormat
import java.util.Date


object Test {
	def main(args: Array[String]): Unit = {

//		val time = "2020-10-31T08:04:34+08:00"
val time ="2020-09-06T20:44:27-07:00"
		val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss-07:00")

		// org.joda.time.DateTime类
		val date = new DateTime(time).toDate

		val dt = sdf.format(date)

		val tempTime: Long = sdf.parse(dt).getTime

		println(tempTime)

		println(sdf.format(new Date(tempTime)))
	}
}

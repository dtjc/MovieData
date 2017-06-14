import java.util.*
import ExcelWrite.NotWriteToExcel

/**
 * Created by dtjc on 2017/6/6.
 */

data class MovieInfo(var title: String = "null", var name: String = "", var rating: Double = -1.0,
                     var directors: String = "", var actors: String = "", var writers: String = "", var format: String = "",
                     var genres: String = "", var duration: Int = -1, var date: String = "", var period: String = "",
                     var countries: String = "", var boxOffice: Double = -1.0, var firm: String = "",
                     var mark: String = "")

data class RoutingData(var str: String, val type: Int,val flag: String = "null", var fre: Int = 0)

data class YienMovie(val MovieName: String, val BoxOffice: Double,val Area: String)

data class ProxyHost(val ip: String,val port: Int)
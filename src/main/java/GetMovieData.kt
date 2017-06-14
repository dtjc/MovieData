
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.jsoup.Jsoup
import java.net.URI
import java.net.URL
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

/**
 * Created by dtjc on 2017/6/7.
 */
internal object GetMovieData {

    //    http://movie.mtime.com/boxoffice/?year=2015&area=china&type=MovieRankingYear&category=all&page=4&display=list&timestamp=1496841174620&version=07bb781100018dd58eafc3b35d42686804c6df8d&dataType=json
    val MTIME_URL_PART1 = "http://movie.mtime.com/boxoffice/?"
    val MTIME_URL_PART2 = "&area=china&category=all&display=list&version=07bb781100018dd58eafc3b35d42686804c6df8d&dataType=json&page="
    val YIEN_URL = "http://www.cbooo.cn/BoxOffice/getInland"
    private val DOUBAN_MOVIE_SEARCH_API = "https://api.douban.com/v2/movie/search?q="

    val splitChar = '/'
    val TYPE_YIEN_PAGE = 5
    val TYPE_MTIME_PAGE = 4
    val TYPE_MTIME_DETAIL = 3
    val TYPE_DOUBAN_SEARCH = 2
    val TYPE_DOUBAN_DETAIL = 1

    private val urlQueue = LinkedBlockingQueue<RoutingData>()
    private val handleQueue = LinkedBlockingQueue<RoutingData>()
    private val movieMap = ConcurrentHashMap<String, MovieInfo>()
    private var threadFlag = 0
    private val totalThread = AtomicInteger(2)
    private var allDone = 0
    private val maxRequest = 16
    private val sleepTime = Array(7) { (it + 1) * 2500L }
    private val userAgent = arrayOf("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36 LBBROWSER",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36")

    val spaceRegex = "\\s+".toRegex()

    @JvmStatic fun main(args: Array<String>) {

        setAllDown()

        for (y in 2015..2017) {
            for (page in 0..9) {
                val url = "${MTIME_URL_PART1}type=MovieRankingYear&year=$y$MTIME_URL_PART2$page&timestamp="
                urlQueue.put(RoutingData(url, TYPE_MTIME_PAGE))
            }
        }
        for (page in 0 .. 9){
            val top100Url = "${MTIME_URL_PART1}type=MovieRankingHistory$MTIME_URL_PART2$page&timestamp="
            urlQueue.put(RoutingData(top100Url, TYPE_MTIME_PAGE))
        }

//        for (i in 1 .. 5){
//            val url = "$YIEN_URL?pIndex=$i&t=0"
//            urlQueue.put(RoutingData(url, TYPE_YIEN_PAGE))
//        }

        startRequestThread()
        startHandleThread()

        while (Thread.activeCount() > 2){
            Thread.sleep(sleepTime[sleepTime.size - 1] shl 1)
        }

        //handleData
        val movieList: List<MovieInfo> = movieMap.map { it.value }
        movieList.parallelStream().forEach {
            if (it.title != it.name)    it.mark += "name$splitChar"
            if (it.rating < 0)  it.mark += "rating$splitChar"
            if (it.directors == "") it.mark += "directors$splitChar"
            if (it.writers == "")   it.mark += "writer$splitChar"
            if (it.actors == "")    it.mark += "actors$splitChar"
            val regex = "\\s*(.+?$splitChar)*[^$splitChar]+\\s*".toRegex()
            if (!it.format.matches(regex))   it.mark += "format$splitChar"
            if (!it.genres.matches(regex))  it.mark += "genres$splitChar"
            if (it.duration < 0)    it.mark += "duration$splitChar"
            if (!it.date.matches("\\d{4}-\\d{2}-\\d{2}".toRegex()))  it.mark += "date$splitChar"
            if (!it.countries.matches(regex))   it.mark += "countries$splitChar"
            if (it.boxOffice < 0)   it.mark += "boxoffice$splitChar"
            if (it.firm == "")  it.mark += "firm$splitChar"
            if (it.mark.isNotEmpty()) it.mark.trim(splitChar)
        }
        ExcelWrite.writeToExcelFile("D:/cache/movieData.xlsx", movieList ,MovieInfo::class.java,"sheet1",1,0)
    }

    fun startRequestThread(){
        Thread{
            val flagOffset = totalThread.decrementAndGet()
            var last = false
            while (flagOffset > -1){
                if (urlQueue.size == 0){
                    if (handleQueue.size == 0){
                        if (setThreadFlag(flagOffset,true) == allDone){
                            break
                        }
                        last = true
                    }
                    Thread.sleep(sleepTime[sleepTime.size shr 1])
                    continue
                }
                if (last){
                    setThreadFlag(flagOffset,false)
                    last = false
                }
                val data = urlQueue.poll()
                data ?: continue
                val k = Random().nextInt(sleepTime.size)
                println(sleepTime[k])
                Thread.sleep(sleepTime[k])
                var url = data.str
                if (data.type == TYPE_MTIME_PAGE){
                    url += System.currentTimeMillis().toString()
                }
                data.fre++
                val str = getResponseStr(url)
                if (str != null){
                    data.str = str
                    handleQueue.put(data)
                }else if (data.fre < maxRequest){
                    urlQueue.put(data)
                }
            }
        }.start()
    }

    fun startHandleThread(){
        Thread{
            val flagOffset = totalThread.decrementAndGet()
            var last = false
            while (flagOffset > -1){
                if (handleQueue.size == 0){
                    if (urlQueue.size == 0){
                        if (setThreadFlag(flagOffset,true) == allDone){
                            break
                        }
                        last = true
                    }
                    Thread.sleep(sleepTime[sleepTime.size-1])
                    continue
                }
                if (last){
                    setThreadFlag(flagOffset,false)
                    last = false
                }
                val data = handleQueue.poll()
                data ?: continue
                when(data.type){
                    TYPE_MTIME_PAGE -> handleMTimePage(data.str)
                    TYPE_DOUBAN_SEARCH -> handleDoubanSearch(data)
                    TYPE_DOUBAN_DETAIL -> handleDoubanDetail(data)
                    TYPE_MTIME_DETAIL -> handleMTimeDetail(data)
                    TYPE_YIEN_PAGE -> handleYienPage(data)
                }
            }
        }.start()
    }

    fun handleMTimeDetail(data: RoutingData){
        val doc = Jsoup.parse(data.str)
        val info1 = doc?.select("div.base_r div.clearfix")?.first()?.select(".pt15 dl.info_l dd")
        val info2 = doc?.select("div.otherbox")?.first()?.select(".__r_c_")?.first()?.text()?.split("-")
        var countries = ""
        var firm = ""
        var format = ""
        info1?.forEach {
            val txt = it.text()
            if (txt.contains("国家地区")){
                it.select("a")?.forEach {
                    countries = "$countries${it.text()}$splitChar"
                }
            }else if (txt.contains("发行公司")){
                firm = it.select("a")?.first()?.text() ?: ""
            }
        }
        if (info2 != null){
            format = info2[info2.size - 1]
        }
        countries = countries.trim(splitChar)
        val movie = movieMap[data.flag] ?: MovieInfo(name = data.flag)
        synchronized(movie){
            movie.countries = countries.replace(spaceRegex,"")
            movie.firm = firm.replace(spaceRegex,"")
            movie.format = format.replace(spaceRegex,"")
        }
    }

    fun handleDoubanDetail(data: RoutingData){
        val doc = Jsoup.parse(data.str)
        val infoDiv = doc?.select("#info")?.first()
        val attrs = infoDiv?.select("span.attrs")
        val genresSpan = infoDiv?.select("span[property=\"v:genre\"]")
        val rating = doc?.select("#interest_sectl div.rating_self strong")?.text()?.toDoubleOrNull() ?: -1.0
        val duration = infoDiv?.select("span[property=\"v:runtime\"]")?.attr("content")?.toIntOrNull() ?: -1
        var date = infoDiv?.select("span[property=\"v:initialReleaseDate\"]")?.text()
        if (date != null && date.length >= 10 ){
            date = date.replace(spaceRegex,"").substring(0,10)
        }
        var directors = ""
        var actors = ""
        var writers = ""
        var genres = ""
        attrs?.forEach {
            it.select("a")?.forEach {
                val rel = it.attr("rel")
                if (rel == "v:directedBy"){
                    directors += "${it.text()}$splitChar"
                }else if (rel == "v:starring"){
                    actors += "${it.text()}$splitChar"
                }else{
                    writers += "${it.text()}$splitChar"
                }
            }
        }
        genresSpan?.forEach { genres += "${it.text()}$splitChar" }
        if (directors.isNotEmpty()) directors = directors.trim(splitChar)
        if (actors.isNotEmpty())    actors = actors.trim(splitChar)
        if (writers.isNotEmpty())   writers = writers.trim(splitChar)
        if (genres.isNotEmpty())    genres = genres.trim(splitChar)
        val movie = movieMap[data.flag] ?: MovieInfo(name = data.flag)
        synchronized(movie){
            movie.duration = duration
            movie.rating = rating
            movie.directors = directors.replace(spaceRegex,"")
            movie.actors = actors.replace(spaceRegex,"")
            movie.writers = writers.replace(spaceRegex,"")
            movie.genres = genres.replace(spaceRegex,"")
            movie.date = date ?: ""
        }
    }

    fun handleDoubanSearch(data: RoutingData){
        val titleRegex = "\"title\":\\s\".+?\""
        val titlePt = Pattern.compile(titleRegex)
        val altRegex = "\"alt\":\\s\"https:\\\\/\\\\/movie[.]douban[.]com\\\\/subject\\\\/\\d+\\\\/\""
        val altPt = Pattern.compile(altRegex)
        val unicodeRegex = "\\\\u\\w{4}"
        val unicodePt = Pattern.compile(unicodeRegex)
        val resStr = data.str
        val altMatcher = altPt.matcher(resStr)
        val titleMatcher = titlePt.matcher(resStr)
        if (altMatcher.find())   {
            var alt = altMatcher.group()
            alt = alt.replace("\\","")
            alt = alt.substring(8,alt.length - 1)
            urlQueue.put(RoutingData(alt, TYPE_DOUBAN_DETAIL,data.flag))
        }
        if (titleMatcher.find()){
            var title = titleMatcher.group()
            title = title.substring(10,title.length-1)
            val unicodeM = unicodePt.matcher(title)
            while (unicodeM.find()){
                val utf8Code = unicodeM.group()
                val hexStr = utf8Code.substring(2)
                val c = Integer.parseInt(hexStr,16).toChar().toString()
                title = title.replace(utf8Code,c)
            }
            val movie = movieMap[data.flag] ?: MovieInfo(name = data.flag)
            synchronized(movie) {
                movie.title = title
            }
        }
    }

    fun handleMTimePage(str: String){
        var html = str
        val pat = Pattern.compile("\"html\":\".+\",\"",Pattern.DOTALL)
        val m = pat.matcher(html)
        if (m.find()){
            html = m.group()
            html = html.substring(8,html.length - 3)
            html = html.replace("\\","")
        }
        val doc = Jsoup.parse(html)
        val list = doc?.select("div.boxofficelist dd div.movietopmod")
        list?.forEach {
            val txtBox = it.select("div.txtbox")?.first()
            val totalBox = it.select("div.totalbox")?.first()
            val a = txtBox?.select("h3 a")?.first()
            var url = a?.attr("href")
            val name = a?.text() ?: "null"
            val p = totalBox?.select("p.totalnum")
            if (name == "null") return@forEach
            var num = p?.select("strong")?.first()?.text()?.toDoubleOrNull() ?: -1.0
            val ptxt = p?.text() ?: ""
            if (ptxt.contains('亿')) num *= 10000
            val movie = MovieInfo(name = name,boxOffice = num)
//            val movie = movieMap[name] ?: MovieInfo(name = name,boxOffice = num)
            movieMap.put(name,movie)
            if (url != null){
                urlQueue.put(RoutingData(url, TYPE_MTIME_DETAIL,name))
            }
            url = DOUBAN_MOVIE_SEARCH_API + movie.name
            urlQueue.put(RoutingData(url,TYPE_DOUBAN_SEARCH,name))
        }
    }

    fun handleYienPage(data: RoutingData){
        val gson = GsonBuilder().create()
        val type = object : TypeToken<List<YienMovie>>() {}.type
        val yienMovies = gson.fromJson<List<YienMovie>>(data.str,type)
        yienMovies.forEach {
            val movie = MovieInfo(name = it.MovieName, boxOffice = it.BoxOffice, countries = it.Area)
            movieMap.put(it.MovieName,movie)
            println("${it.MovieName},${it.BoxOffice},${it.Area}")
            val url = DOUBAN_MOVIE_SEARCH_API + movie.name
            urlQueue.put(RoutingData(url,TYPE_DOUBAN_SEARCH,movie.name))
        }
    }

    fun setAllDown(){
        if (totalThread.get() > 32){
            throw IllegalArgumentException("totalThread can not be greater than 32")
        }
        for (i in 0 until totalThread.get()){
            allDone = allDone or (1 shl i)
        }
    }

    @Synchronized fun setThreadFlag(offset: Int, wait: Boolean): Int{
        if (wait){
            threadFlag = threadFlag or ( 1 shl offset )
        }else if (offset in 0 .. 30){
            threadFlag = threadFlag and (Int.MIN_VALUE + 1 + (1 shl offset) )
        }else if (threadFlag < 0){
            threadFlag = -threadFlag
        }
        return threadFlag
    }

    fun getResponseStr(urlStr: String): String? {
        try {
            val str: String?
            val k = Random().nextInt(userAgent.size)
            val url = URL(urlStr)
            val uri = URI(url.protocol, url.host, url.path, url.query, null)
            val httpGet = HttpGet(uri)
            httpGet.setHeader("User-Agent", userAgent[k])
            val defaultTimeout = 20000
            val config = RequestConfig.custom()
                    .setConnectTimeout(defaultTimeout)
                    .setSocketTimeout(defaultTimeout)
                    .setConnectionRequestTimeout(defaultTimeout)
                    .build()
            val client = HttpClientBuilder.create()
                    .setDefaultRequestConfig(config)
                    .build()
            val response: HttpResponse? = client.execute(httpGet)
            if(response?.statusLine?.statusCode == 200){
                str = EntityUtils.toString(response.entity)
                println("url:$url , success")
            }else{
                println("url:$url , fail")
                str = null
            }
            return str
        } catch (e: Throwable) {
            e.printStackTrace()
            println("url:$urlStr , fail")
            return null
        }
    }
}
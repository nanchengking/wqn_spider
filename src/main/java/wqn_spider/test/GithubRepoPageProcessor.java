package wqn_spider.test;

import java.util.List;

import org.apache.log4j.Logger;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.JsonFilePipeline;
import us.codecraft.webmagic.processor.PageProcessor;

public class GithubRepoPageProcessor implements PageProcessor {

    private static Logger logger = Logger.getLogger(GithubRepoPageProcessor.class);
    private Site site = Site.me().setRetryTimes(3).setSleepTime(100);

    @Override
    public void process(Page page) {
        List<String> hrefs = page.getHtml().css("div.fm-movie-title").$("a", "href").all();
        for (String i : hrefs) {
            // http://dianying.fm/movie/da-hua-xi-you-zhi-yue-guang-bao-he/
            i = "http://dianying.fm/" + i;
        }
        page.addTargetRequests(hrefs);
        page.putField("name", page.getHtml().xpath("//h3/a[@name='title']/text()").toString());
        page.putField("content", page.getHtml().css("div.fm-summary").toString());
        if (page.getResultItems().get("name") == null) {
            page.setSkip(true);
        }
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new GithubRepoPageProcessor()).addUrl("http://dianying.fm/search/?genre=%E5%96%9C%E5%89%A7")
               .addPipeline(new JsonFilePipeline("target/json")) .thread(5).run();
        try {
            WordCount.runHadoop();
        } catch (Exception e) {
            logger.error(e.getMessage());
            logger.error("运行hadoop出错");
        }
        logger.info("运行hadoop成功！！！");
    }
}
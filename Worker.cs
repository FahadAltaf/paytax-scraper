using HtmlAgilityPack;
using PuppeteerSharp;

namespace paytax.erie.gov_scraper;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var lines = File.ReadLines("file.txt").ToList();
        List<DataModel> entries = new List<DataModel>();
        foreach (var item in lines)
        {
            var first = item.Split(" ").FirstOrDefault();
            entries.Add(new DataModel { FullAddress = item, HouseNo = first, Street = item.Substring(first.Length + 1) });
        }
   

        int pages = (entries.Count + 10 - 1) / 10;
        List<Task> tasks = new List<Task>();
        for (int count = 1; count <= pages; ++count)
        {
            int index = count - 1;
            var data = entries.Skip(index * 10).Take(10).ToList();

            Task newTask = Task.Factory.StartNew(() => {  Scrape(data).Wait(); });
            tasks.Add(newTask);

            if (count % 5 == 0 || count == pages)
            {
                foreach (Task task in tasks)
                {
                    while (!task.IsCompleted)
                    { }
                }
            }
        }
        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            await Task.Delay(1000, stoppingToken);
        }
    }

    public async Task<List<DataModel>> Scrape(List<DataModel> entries)
    {
        try
        {
            string url = "https://paytax.erie.gov/(S(tsuym4i1s14qiuyp2z1fequh))/WebPortal/WEB_PT_MAIN.aspx?command=";
       
            using var browserFetcher = new BrowserFetcher();
            await browserFetcher.DownloadAsync(BrowserFetcher.DefaultChromiumRevision);
            var browser = await Puppeteer.LaunchAsync(new LaunchOptions
            {
                DefaultViewport = null,
                Headless = false
            });
            var page = (await browser.PagesAsync())[0];
            foreach (var entry in entries)
            {
                await page.GoToAsync(url);
                await page.WaitForTimeoutAsync(2000);
                await page.ClickAsync("#Web_PT_Header_MyMenuManager_mnuMainn3 > table > tbody > tr > td > a");
                await page.WaitForTimeoutAsync(3000);
                await page.ClickAsync("#objWP_reportparameterstyle_ESearchManager1_rdblStyles_0");
                await page.WaitForTimeoutAsync(2000);
                await page.ClickAsync("#objWP_reportparameterstyle_ESearchManager1_cmdLoadStyleContent");
                await page.WaitForTimeoutAsync(3000);
                await page.ClickAsync("#objWP_reportparameterstyle_ESearchManager1_Web_CO_SearchPanel1_txt_GFD0", new PuppeteerSharp.Input.ClickOptions { ClickCount = 3 });
                await page.TypeAsync("#objWP_reportparameterstyle_ESearchManager1_Web_CO_SearchPanel1_txt_GFD0", entry.HouseNo, new PuppeteerSharp.Input.TypeOptions { Delay = 100 });

                await page.ClickAsync("#objWP_reportparameterstyle_ESearchManager1_Web_CO_SearchPanel1_txt_GFD1", new PuppeteerSharp.Input.ClickOptions { ClickCount = 3 });
                await page.TypeAsync("#objWP_reportparameterstyle_ESearchManager1_Web_CO_SearchPanel1_txt_GFD1", entry.Street, new PuppeteerSharp.Input.TypeOptions { Delay = 100 });


                await page.ClickAsync("#objWP_reportparameterstyle_ESearchManager1_Web_CO_SearchPanel1_btnGo");
                await page.WaitForTimeoutAsync(3000);

                var html = await page.GetContentAsync();
                HtmlDocument doc = new HtmlDocument();
                doc.LoadHtml(html);

                var table = doc.DocumentNode.SelectSingleNode("//*[@id=\"objWP_reportparameterstyle_ESearchManager1_Web_CO_SearchPanel1_grdResult\"]/tbody");
                if (table != null && table.ChildNodes.Where(x => x.Name == "tr").Count() > 0)
                {
                    await page.ClickAsync("#objWP_reportparameterstyle_ESearchManager1_cmdFinish");
                    await page.WaitForTimeoutAsync(3500);

                 var frame =   page.Frames.FirstOrDefault(x => x.Name.Contains("bobjid"));
                    

                    try
                    {
                        if (frame != null)
                        {
                            await frame.ClickAsync("#Text5 > div > div > a");
                            await frame.WaitForTimeoutAsync(5000);

                            frame = page.Frames.FirstOrDefault(x => x.Name.Contains("bobjid"));

                            html = await frame.GetContentAsync();
                            doc.LoadHtml(html);

                            var owner1 = doc.DocumentNode.SelectNodes("//div[@id='Ownername1']");
                            if (owner1 != null)
                            {
                                var owner = owner1.LastOrDefault();
                                var bookPage = doc.DocumentNode.SelectNodes("//div[@id='bookpage1']").LastOrDefault();
                                if (owner != null)
                                {
                                    entry.OwnerName = owner.InnerText.Replace("&nbsp;", "  ").Replace("\n", "").Trim();
                                    var bookPag = bookPage.InnerText.Replace("&nbsp;", "  ").Replace("\n", "").Trim();
                                    var parts = bookPag.Split("*");
                                    entry.BookPage = parts[0].Trim();
                                    entry.Date = parts[1].Trim();
                                     _logger.LogInformation($"Address: {entry.HouseNo} {entry.Street} ===> {entry.OwnerName} - {entry.BookPage}");
                                 
                                }
                                else
                                {
                                    _logger.LogWarning($"Owner details not found for Address: {entry.HouseNo} {entry.Street}");
                                }
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("error");
                    }

                }
                else
                {
                    _logger.LogWarning($"No result found for Address: {entry.HouseNo} {entry.Street}");
                }
            }
            
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
        }

        return entries;
    }
}


public class DataModel
{
    public string FullAddress { get; set; }
    public string Street { get; set; }
    public string HouseNo { get; set; }
    public string OwnerName { get; set; }
    public string BookPage { get; set; }
    public string Date { get; set; }
}

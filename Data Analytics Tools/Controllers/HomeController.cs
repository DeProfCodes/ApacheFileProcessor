using Data_Analytics_Tools.BusinessLogic;
using Data_Analytics_Tools.Helpers;
using Data_Analytics_Tools.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Data_Analytics_Tools.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private ApacheLogFilesHelper apacheHelper;

        public HomeController(ILogger<HomeController> logger, IBusinessLogicData dataIO)
        {
            _logger = logger;
            apacheHelper = new ApacheLogFilesHelper(dataIO);
        }

        public IActionResult Index()
        {
            return View();
        }

        public async Task<IActionResult> ApacheFilesDownload()
        {
            var apacheLogsDirectory = @"F:\Proficient\DATA\Apache Log Files";
            apacheHelper.SetApacheLogsDirectory(apacheLogsDirectory);

          //  await apacheHelper.CreateTablesSchema();
            await apacheHelper.CreateLogFileListForDownload();
            
            try
            {
                apacheHelper.DownloadApacheFilesFromServer();
            }
            catch(Exception e)
            {
                int m = 0;
            }

            return RedirectToAction("Index", "Home");
        }

        public async Task<IActionResult> LoadApacheFilesToMySQL()
        {
            try
            {
                await apacheHelper.CreateTablesSchema();
                await apacheHelper.ImportApacheFilesToMySQL();
            }
            catch (Exception e)
            {
                
            }

            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}

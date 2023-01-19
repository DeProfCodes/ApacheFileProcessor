using Data_Analytics_Tools.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Threading.Tasks;

namespace Data_Analytics_Tools.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options)
        {

        }

        public DbSet<ProcessedApacheFile> ApacheFilesImportProgress { get; set; }

        internal Task FirstOrDefaultAsync()
        {
            throw new NotImplementedException();
        }
    }
}

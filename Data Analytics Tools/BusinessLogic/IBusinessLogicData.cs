using System.Collections.Generic;
using System.Threading.Tasks;

namespace Data_Analytics_Tools.BusinessLogic
{
    public interface IBusinessLogicData
    {
        public Task AddOrUpdateApacheLogFileImport(string filename, bool importComplete, string error);

        public Task DeleteApacheLogFileImport(string filename);

        public Task DeleteApacheLogFileImport(List<string> filenames);

        public Task<List<string>> GetProcessedApacheFiles();
    }
}

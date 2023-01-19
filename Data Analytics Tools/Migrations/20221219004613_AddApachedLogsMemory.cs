using Microsoft.EntityFrameworkCore.Migrations;

namespace Data_Analytics_Tools.Migrations
{
    public partial class AddApachedLogsMemory : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ApacheFilesImportProgress",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Filename = table.Column<int>(type: "int", nullable: false),
                    ImportComplete = table.Column<bool>(type: "bit", nullable: false),
                    ProcessError = table.Column<string>(type: "nvarchar(max)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ApacheFilesImportProgress", x => x.Id);
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ApacheFilesImportProgress");
        }
    }
}

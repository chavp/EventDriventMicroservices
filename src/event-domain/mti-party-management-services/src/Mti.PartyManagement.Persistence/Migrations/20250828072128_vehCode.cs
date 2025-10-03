using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.PartyManagement.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class vehCode : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Code",
                schema: "parties",
                table: "Vehicles",
                type: "character varying(10)",
                maxLength: 10,
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Code",
                schema: "parties",
                table: "Vehicles");
        }
    }
}

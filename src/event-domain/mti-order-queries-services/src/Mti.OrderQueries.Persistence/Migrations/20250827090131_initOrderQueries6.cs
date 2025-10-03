using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.OrderQueries.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class initOrderQueries6 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_ProductDims_ProductCode",
                schema: "order_queries",
                table: "ProductDims");

            migrationBuilder.CreateIndex(
                name: "IX_ProductDims_ProductCode",
                schema: "order_queries",
                table: "ProductDims",
                column: "ProductCode",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_ProductDims_ProductCode",
                schema: "order_queries",
                table: "ProductDims");

            migrationBuilder.CreateIndex(
                name: "IX_ProductDims_ProductCode",
                schema: "order_queries",
                table: "ProductDims",
                column: "ProductCode");
        }
    }
}

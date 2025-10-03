using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.OrderQueries.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class initOrderQueries7 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_ProductDims_ProductCode",
                schema: "order_queries",
                table: "ProductDims");

            migrationBuilder.AddColumn<string>(
                name: "Products_TenantId",
                schema: "order_queries",
                table: "ProductDims",
                type: "character varying(200)",
                maxLength: 200,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "Parties_TenantId",
                schema: "order_queries",
                table: "PartyDims",
                type: "character varying(200)",
                maxLength: 200,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "Orders_TenantId",
                schema: "order_queries",
                table: "OrderDims",
                type: "character varying(200)",
                maxLength: 200,
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_ProductDims_ProductCode_Products_TenantId",
                schema: "order_queries",
                table: "ProductDims",
                columns: new[] { "ProductCode", "Products_TenantId" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_ProductDims_ProductCode_Products_TenantId",
                schema: "order_queries",
                table: "ProductDims");

            migrationBuilder.DropColumn(
                name: "Products_TenantId",
                schema: "order_queries",
                table: "ProductDims");

            migrationBuilder.DropColumn(
                name: "Parties_TenantId",
                schema: "order_queries",
                table: "PartyDims");

            migrationBuilder.DropColumn(
                name: "Orders_TenantId",
                schema: "order_queries",
                table: "OrderDims");

            migrationBuilder.CreateIndex(
                name: "IX_ProductDims_ProductCode",
                schema: "order_queries",
                table: "ProductDims",
                column: "ProductCode",
                unique: true);
        }
    }
}

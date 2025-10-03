using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.OrderQueries.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class initOrderQueries4 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<long>(
                name: "OrderItemRoleSeq",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                type: "bigint",
                nullable: false,
                defaultValue: 0L);

            migrationBuilder.AddColumn<string>(
                name: "OrderItemRoleTypeCode",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                type: "character varying(256)",
                maxLength: 256,
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "OrderItemRoleSeq",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims");

            migrationBuilder.DropColumn(
                name: "OrderItemRoleTypeCode",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims");
        }
    }
}

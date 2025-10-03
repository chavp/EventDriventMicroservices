using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.OrderQueries.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class initOrderQueries2 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_ContactMechanismDims_PartyDims_PartyDimId",
                schema: "order_queries",
                table: "ContactMechanismDims");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_ApplicationDims_ApplicationDimId",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_InsuredAssetDims_InsuredAssetDimId",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_OrderDims_OrderDimId",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_ProductDims_ProductDimId",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemPartyRoleDims_OrderItemFacts_OrderItemFactId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemPartyRoleDims_PartyDims_PartyDimId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims");

            migrationBuilder.RenameColumn(
                name: "PartyDimId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "PartyDimKey");

            migrationBuilder.RenameColumn(
                name: "OrderItemFactId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "OrderItemFactKey");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemPartyRoleDims_PartyDimId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "IX_OrderItemPartyRoleDims_PartyDimKey");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemPartyRoleDims_OrderItemFactId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "IX_OrderItemPartyRoleDims_OrderItemFactKey");

            migrationBuilder.RenameColumn(
                name: "ProductDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "ProductDimKey");

            migrationBuilder.RenameColumn(
                name: "OrderDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "OrderDimKey");

            migrationBuilder.RenameColumn(
                name: "InsuredAssetDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "InsuredAssetDimKey");

            migrationBuilder.RenameColumn(
                name: "ApplicationDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "ApplicationDimKey");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_ProductDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_ProductDimKey");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_OrderDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_OrderDimKey");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_InsuredAssetDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_InsuredAssetDimKey");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_ApplicationDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_ApplicationDimKey");

            migrationBuilder.RenameColumn(
                name: "PartyDimId",
                schema: "order_queries",
                table: "ContactMechanismDims",
                newName: "PartyDimKey");

            migrationBuilder.RenameIndex(
                name: "IX_ContactMechanismDims_PartyDimId",
                schema: "order_queries",
                table: "ContactMechanismDims",
                newName: "IX_ContactMechanismDims_PartyDimKey");

            migrationBuilder.AddForeignKey(
                name: "FK_ContactMechanismDims_PartyDims_PartyDimKey",
                schema: "order_queries",
                table: "ContactMechanismDims",
                column: "PartyDimKey",
                principalSchema: "order_queries",
                principalTable: "PartyDims",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_ApplicationDims_ApplicationDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "ApplicationDimKey",
                principalSchema: "order_queries",
                principalTable: "ApplicationDims",
                principalColumn: "Key");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_InsuredAssetDims_InsuredAssetDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "InsuredAssetDimKey",
                principalSchema: "order_queries",
                principalTable: "InsuredAssetDims",
                principalColumn: "Key");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_OrderDims_OrderDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "OrderDimKey",
                principalSchema: "order_queries",
                principalTable: "OrderDims",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_ProductDims_ProductDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "ProductDimKey",
                principalSchema: "order_queries",
                principalTable: "ProductDims",
                principalColumn: "Key");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemPartyRoleDims_OrderItemFacts_OrderItemFactKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                column: "OrderItemFactKey",
                principalSchema: "order_queries",
                principalTable: "OrderItemFacts",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemPartyRoleDims_PartyDims_PartyDimKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                column: "PartyDimKey",
                principalSchema: "order_queries",
                principalTable: "PartyDims",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_ContactMechanismDims_PartyDims_PartyDimKey",
                schema: "order_queries",
                table: "ContactMechanismDims");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_ApplicationDims_ApplicationDimKey",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_InsuredAssetDims_InsuredAssetDimKey",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_OrderDims_OrderDimKey",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemFacts_ProductDims_ProductDimKey",
                schema: "order_queries",
                table: "OrderItemFacts");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemPartyRoleDims_OrderItemFacts_OrderItemFactKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims");

            migrationBuilder.DropForeignKey(
                name: "FK_OrderItemPartyRoleDims_PartyDims_PartyDimKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims");

            migrationBuilder.RenameColumn(
                name: "PartyDimKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "PartyDimId");

            migrationBuilder.RenameColumn(
                name: "OrderItemFactKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "OrderItemFactId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemPartyRoleDims_PartyDimKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "IX_OrderItemPartyRoleDims_PartyDimId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemPartyRoleDims_OrderItemFactKey",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                newName: "IX_OrderItemPartyRoleDims_OrderItemFactId");

            migrationBuilder.RenameColumn(
                name: "ProductDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "ProductDimId");

            migrationBuilder.RenameColumn(
                name: "OrderDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "OrderDimId");

            migrationBuilder.RenameColumn(
                name: "InsuredAssetDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "InsuredAssetDimId");

            migrationBuilder.RenameColumn(
                name: "ApplicationDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "ApplicationDimId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_ProductDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_ProductDimId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_OrderDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_OrderDimId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_InsuredAssetDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_InsuredAssetDimId");

            migrationBuilder.RenameIndex(
                name: "IX_OrderItemFacts_ApplicationDimKey",
                schema: "order_queries",
                table: "OrderItemFacts",
                newName: "IX_OrderItemFacts_ApplicationDimId");

            migrationBuilder.RenameColumn(
                name: "PartyDimKey",
                schema: "order_queries",
                table: "ContactMechanismDims",
                newName: "PartyDimId");

            migrationBuilder.RenameIndex(
                name: "IX_ContactMechanismDims_PartyDimKey",
                schema: "order_queries",
                table: "ContactMechanismDims",
                newName: "IX_ContactMechanismDims_PartyDimId");

            migrationBuilder.AddForeignKey(
                name: "FK_ContactMechanismDims_PartyDims_PartyDimId",
                schema: "order_queries",
                table: "ContactMechanismDims",
                column: "PartyDimId",
                principalSchema: "order_queries",
                principalTable: "PartyDims",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_ApplicationDims_ApplicationDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "ApplicationDimId",
                principalSchema: "order_queries",
                principalTable: "ApplicationDims",
                principalColumn: "Key");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_InsuredAssetDims_InsuredAssetDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "InsuredAssetDimId",
                principalSchema: "order_queries",
                principalTable: "InsuredAssetDims",
                principalColumn: "Key");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_OrderDims_OrderDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "OrderDimId",
                principalSchema: "order_queries",
                principalTable: "OrderDims",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemFacts_ProductDims_ProductDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "ProductDimId",
                principalSchema: "order_queries",
                principalTable: "ProductDims",
                principalColumn: "Key");

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemPartyRoleDims_OrderItemFacts_OrderItemFactId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                column: "OrderItemFactId",
                principalSchema: "order_queries",
                principalTable: "OrderItemFacts",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_OrderItemPartyRoleDims_PartyDims_PartyDimId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                column: "PartyDimId",
                principalSchema: "order_queries",
                principalTable: "PartyDims",
                principalColumn: "Key",
                onDelete: ReferentialAction.Cascade);
        }
    }
}

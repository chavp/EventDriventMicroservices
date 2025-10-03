using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.OrderQueries.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class initOrderQueries : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "order_queries");

            migrationBuilder.CreateTable(
                name: "ApplicationDims",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    ApplicationOriginalId = table.Column<long>(type: "bigint", nullable: true),
                    ApplicationTransID = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ApplicationStatus = table.Column<string>(type: "character varying(5)", maxLength: 5, nullable: true),
                    ApplicationPolicyType = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    ApplicationPolicyNumber = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ApplicationPolicyPreviousNumber = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ApplicationPolicyEffectiveDate = table.Column<DateOnly>(type: "date", nullable: true),
                    ApplicationPolicyExpiryDate = table.Column<DateOnly>(type: "date", nullable: true),
                    ApplicationRemark = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true),
                    ApplicationRefNoticeNo = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ApplicationRefDetailNo = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ApplicationStatusMessage = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    ApplicationRefQuotation = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    ApplicationSource = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    ApplicationSystemId = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    ApplicationCustomerInfoNo = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    ApplicationPayPlan = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    ApplicationCollateralNo = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ApplicationDims", x => x.Key);
                });

            migrationBuilder.CreateTable(
                name: "InsuredAssetDims",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    AssetTypeCode = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    AssetId = table.Column<Guid>(type: "uuid", nullable: true),
                    AssetName = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    VehicleBrand = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    VehicleModel = table.Column<string>(type: "character varying(600)", maxLength: 600, nullable: true),
                    VehicleColor = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    VehicleRegisterNo = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    VehicleRegisterProvince = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    VehicleRegisterYear = table.Column<int>(type: "integer", nullable: true),
                    VehicleChassis = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    VehicleCc = table.Column<float>(type: "real", nullable: true),
                    VehicleSeat = table.Column<float>(type: "real", nullable: true),
                    VehicleWeight = table.Column<float>(type: "real", nullable: true),
                    VehicleTonnage = table.Column<float>(type: "real", nullable: true),
                    VehicleEngine = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    VehiclePassenger = table.Column<int>(type: "integer", nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_InsuredAssetDims", x => x.Key);
                });

            migrationBuilder.CreateTable(
                name: "OrderDims",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderId = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderNumber = table.Column<string>(type: "text", nullable: false),
                    OrderSaleDate = table.Column<DateOnly>(type: "date", nullable: false),
                    OrderLoanNumber = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderDims", x => x.Key);
                });

            migrationBuilder.CreateTable(
                name: "PartyDims",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    PartyTypeCode = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    PartyId = table.Column<Guid>(type: "uuid", nullable: true),
                    OrganizationName = table.Column<string>(type: "character varying(400)", maxLength: 400, nullable: true),
                    OrganizationReference = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    LegalOrganizationFederalTaxIdNumber = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    PersonFirstName = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PersonMiddleName = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PersonLastName = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    PersonCardId = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    PersonBirthDate = table.Column<DateOnly>(type: "date", nullable: true),
                    PersonHeight = table.Column<int>(type: "integer", nullable: true),
                    PersonWeight = table.Column<int>(type: "integer", nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartyDims", x => x.Key);
                });

            migrationBuilder.CreateTable(
                name: "ProductDims",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    ProductId = table.Column<Guid>(type: "uuid", nullable: true),
                    ProductCode = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: false),
                    ProductName = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    ProductCategoryTypeCodes = table.Column<string>(type: "character varying(5000)", maxLength: 5000, nullable: true),
                    ProductCampaign = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ProductPackage = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    ProductWorkshop = table.Column<string>(type: "character varying(10)", maxLength: 10, nullable: true),
                    ProductRefPolicyType = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProductDims", x => x.Key);
                });

            migrationBuilder.CreateTable(
                name: "ContactMechanismDims",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    ContactMechanismId = table.Column<Guid>(type: "uuid", nullable: true),
                    PartyDimId = table.Column<Guid>(type: "uuid", nullable: false),
                    ContactMechanismTypeCode = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    PostalAddressName = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    PostalAddressHouseNumber = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    PostalAddressVillageNumber = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    PostalAddressVillage = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PostalAddressAlley = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PostalAddressRoad = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PostalAddressBuilding = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PostalAddressRoom = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    PostalAddressFloor = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    PostalAddressProvince = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PostalAddressDistrict = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PostalAddressSubDistrict = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    PostalAddressZipCode = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    PostalAddressDisplayName = table.Column<string>(type: "character varying(3000)", maxLength: 3000, nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ContactMechanismDims", x => x.Key);
                    table.ForeignKey(
                        name: "FK_ContactMechanismDims_PartyDims_PartyDimId",
                        column: x => x.PartyDimId,
                        principalSchema: "order_queries",
                        principalTable: "PartyDims",
                        principalColumn: "Key",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "OrderItemFacts",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderItemId = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderItemSeq = table.Column<long>(type: "bigint", nullable: false),
                    OrderItemPrice = table.Column<decimal>(type: "numeric", nullable: false),
                    OrderItemQuantity = table.Column<int>(type: "integer", nullable: false),
                    OrderItemOrderedForId = table.Column<Guid>(type: "uuid", nullable: true),
                    OrderDimId = table.Column<Guid>(type: "uuid", nullable: false),
                    ApplicationDimId = table.Column<Guid>(type: "uuid", nullable: true),
                    ProductDimId = table.Column<Guid>(type: "uuid", nullable: true),
                    InsuredAssetDimId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderItemFacts", x => x.Key);
                    table.ForeignKey(
                        name: "FK_OrderItemFacts_ApplicationDims_ApplicationDimId",
                        column: x => x.ApplicationDimId,
                        principalSchema: "order_queries",
                        principalTable: "ApplicationDims",
                        principalColumn: "Key");
                    table.ForeignKey(
                        name: "FK_OrderItemFacts_InsuredAssetDims_InsuredAssetDimId",
                        column: x => x.InsuredAssetDimId,
                        principalSchema: "order_queries",
                        principalTable: "InsuredAssetDims",
                        principalColumn: "Key");
                    table.ForeignKey(
                        name: "FK_OrderItemFacts_OrderDims_OrderDimId",
                        column: x => x.OrderDimId,
                        principalSchema: "order_queries",
                        principalTable: "OrderDims",
                        principalColumn: "Key",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_OrderItemFacts_ProductDims_ProductDimId",
                        column: x => x.ProductDimId,
                        principalSchema: "order_queries",
                        principalTable: "ProductDims",
                        principalColumn: "Key");
                });

            migrationBuilder.CreateTable(
                name: "OrderItemPartyRoleDims",
                schema: "order_queries",
                columns: table => new
                {
                    Key = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderItemFactId = table.Column<Guid>(type: "uuid", nullable: false),
                    PartyDimId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderItemPartyRoleDims", x => x.Key);
                    table.ForeignKey(
                        name: "FK_OrderItemPartyRoleDims_OrderItemFacts_OrderItemFactId",
                        column: x => x.OrderItemFactId,
                        principalSchema: "order_queries",
                        principalTable: "OrderItemFacts",
                        principalColumn: "Key",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_OrderItemPartyRoleDims_PartyDims_PartyDimId",
                        column: x => x.PartyDimId,
                        principalSchema: "order_queries",
                        principalTable: "PartyDims",
                        principalColumn: "Key",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ContactMechanismDims_PartyDimId",
                schema: "order_queries",
                table: "ContactMechanismDims",
                column: "PartyDimId");

            migrationBuilder.CreateIndex(
                name: "IX_InsuredAssetDims_AssetTypeCode",
                schema: "order_queries",
                table: "InsuredAssetDims",
                column: "AssetTypeCode");

            migrationBuilder.CreateIndex(
                name: "IX_OrderDims_OrderId",
                schema: "order_queries",
                table: "OrderDims",
                column: "OrderId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_OrderDims_OrderLoanNumber",
                schema: "order_queries",
                table: "OrderDims",
                column: "OrderLoanNumber");

            migrationBuilder.CreateIndex(
                name: "IX_OrderDims_OrderSaleDate",
                schema: "order_queries",
                table: "OrderDims",
                column: "OrderSaleDate");

            migrationBuilder.CreateIndex(
                name: "IX_OrderDims_OrderSaleDate_OrderLoanNumber",
                schema: "order_queries",
                table: "OrderDims",
                columns: new[] { "OrderSaleDate", "OrderLoanNumber" });

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemFacts_ApplicationDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "ApplicationDimId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemFacts_InsuredAssetDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "InsuredAssetDimId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemFacts_OrderDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "OrderDimId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemFacts_OrderItemId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "OrderItemId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemFacts_ProductDimId",
                schema: "order_queries",
                table: "OrderItemFacts",
                column: "ProductDimId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemPartyRoleDims_OrderItemFactId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                column: "OrderItemFactId");

            migrationBuilder.CreateIndex(
                name: "IX_OrderItemPartyRoleDims_PartyDimId",
                schema: "order_queries",
                table: "OrderItemPartyRoleDims",
                column: "PartyDimId");

            migrationBuilder.CreateIndex(
                name: "IX_PartyDims_PartyTypeCode",
                schema: "order_queries",
                table: "PartyDims",
                column: "PartyTypeCode");

            migrationBuilder.CreateIndex(
                name: "IX_ProductDims_ProductCategoryTypeCodes",
                schema: "order_queries",
                table: "ProductDims",
                column: "ProductCategoryTypeCodes");

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
            migrationBuilder.DropTable(
                name: "ContactMechanismDims",
                schema: "order_queries");

            migrationBuilder.DropTable(
                name: "OrderItemPartyRoleDims",
                schema: "order_queries");

            migrationBuilder.DropTable(
                name: "OrderItemFacts",
                schema: "order_queries");

            migrationBuilder.DropTable(
                name: "PartyDims",
                schema: "order_queries");

            migrationBuilder.DropTable(
                name: "ApplicationDims",
                schema: "order_queries");

            migrationBuilder.DropTable(
                name: "InsuredAssetDims",
                schema: "order_queries");

            migrationBuilder.DropTable(
                name: "OrderDims",
                schema: "order_queries");

            migrationBuilder.DropTable(
                name: "ProductDims",
                schema: "order_queries");
        }
    }
}

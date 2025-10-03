using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Mti.PartyManagement.Persistence.Migrations
{
    /// <inheritdoc />
    public partial class initParty : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "parties");

            migrationBuilder.CreateTable(
                name: "AgentChannels",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AgentChannels", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "AgentMasters",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AgentMasters", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "AssetRoleTypes",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AssetRoleTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Assets",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Assets", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "ContactMechanismTypes",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ContactMechanismTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Nationalities",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Nationalities", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "PartyRoleTypes",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartyRoleTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "PartyTitles",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    IsOrganization = table.Column<bool>(type: "boolean", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    Code = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true),
                    Name = table.Column<string>(type: "character varying(1000)", maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartyTitles", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Vehicles",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Brand = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Model = table.Column<string>(type: "character varying(600)", maxLength: 600, nullable: true),
                    Color = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    RegisterNo = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    RegisterProvince = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    RegisterYear = table.Column<int>(type: "integer", nullable: true),
                    Chassis = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    Cc = table.Column<float>(type: "real", nullable: true),
                    Seat = table.Column<float>(type: "real", nullable: true),
                    Weight = table.Column<float>(type: "real", nullable: true),
                    Tonnage = table.Column<float>(type: "real", nullable: true),
                    Engine = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    Passenger = table.Column<int>(type: "integer", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Vehicles", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Vehicles_Assets_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "Assets",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "ContactMechanisms",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Seq = table.Column<int>(type: "integer", nullable: false),
                    ContactMechanismTypeId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ContactMechanisms", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ContactMechanisms_ContactMechanismTypes_ContactMechanismTyp~",
                        column: x => x.ContactMechanismTypeId,
                        principalSchema: "parties",
                        principalTable: "ContactMechanismTypes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Parties",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PartyTitleId = table.Column<Guid>(type: "uuid", nullable: true),
                    NationalityId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Parties", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Parties_Nationalities_NationalityId",
                        column: x => x.NationalityId,
                        principalSchema: "parties",
                        principalTable: "Nationalities",
                        principalColumn: "Id");
                    table.ForeignKey(
                        name: "FK_Parties_PartyTitles_PartyTitleId",
                        column: x => x.PartyTitleId,
                        principalSchema: "parties",
                        principalTable: "PartyTitles",
                        principalColumn: "Id");
                });

            migrationBuilder.CreateTable(
                name: "PostalAddresses",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    HouseNumber = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    VillageNumber = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    Village = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    Alley = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    Road = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    Building = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    Room = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    Floor = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    Province = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    District = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    SubDistrict = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    ZipCode = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PostalAddresses", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PostalAddresses_ContactMechanisms_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "ContactMechanisms",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "AssetRoles",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PartyId = table.Column<Guid>(type: "uuid", nullable: false),
                    AssetRoleTypeId = table.Column<Guid>(type: "uuid", nullable: false),
                    AssetId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    EffectiveDate = table.Column<DateOnly>(type: "date", nullable: false),
                    ExpiryDate = table.Column<DateOnly>(type: "date", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AssetRoles", x => x.Id);
                    table.ForeignKey(
                        name: "FK_AssetRoles_AssetRoleTypes_AssetRoleTypeId",
                        column: x => x.AssetRoleTypeId,
                        principalSchema: "parties",
                        principalTable: "AssetRoleTypes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_AssetRoles_Assets_AssetId",
                        column: x => x.AssetId,
                        principalSchema: "parties",
                        principalTable: "Assets",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_AssetRoles_Parties_PartyId",
                        column: x => x.PartyId,
                        principalSchema: "parties",
                        principalTable: "Parties",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Organizations",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "character varying(400)", maxLength: 400, nullable: true),
                    Reference = table.Column<string>(type: "character varying(500)", maxLength: 500, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Organizations", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Organizations_Parties_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "Parties",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PartyContactMechanisms",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PartyId = table.Column<Guid>(type: "uuid", nullable: false),
                    ContactMechanismId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    EffectiveDate = table.Column<DateOnly>(type: "date", nullable: false),
                    ExpiryDate = table.Column<DateOnly>(type: "date", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartyContactMechanisms", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PartyContactMechanisms_ContactMechanisms_ContactMechanismId",
                        column: x => x.ContactMechanismId,
                        principalSchema: "parties",
                        principalTable: "ContactMechanisms",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_PartyContactMechanisms_Parties_PartyId",
                        column: x => x.PartyId,
                        principalSchema: "parties",
                        principalTable: "Parties",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PartyRoles",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    PartyId = table.Column<Guid>(type: "uuid", nullable: false),
                    PartyRoleTypeId = table.Column<Guid>(type: "uuid", nullable: false),
                    CreatedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: false),
                    ModifiedOnUtc = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    ModifiedBy = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    Revision = table.Column<long>(type: "bigint", nullable: false),
                    EffectiveDate = table.Column<DateOnly>(type: "date", nullable: false),
                    ExpiryDate = table.Column<DateOnly>(type: "date", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartyRoles", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PartyRoles_Parties_PartyId",
                        column: x => x.PartyId,
                        principalSchema: "parties",
                        principalTable: "Parties",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_PartyRoles_PartyRoleTypes_PartyRoleTypeId",
                        column: x => x.PartyRoleTypeId,
                        principalSchema: "parties",
                        principalTable: "PartyRoleTypes",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "People",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    FirstName = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    MiddleName = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    LastName = table.Column<string>(type: "character varying(300)", maxLength: 300, nullable: true),
                    CardId = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    BirthDate = table.Column<DateOnly>(type: "date", nullable: true),
                    Height = table.Column<int>(type: "integer", nullable: true),
                    Weight = table.Column<int>(type: "integer", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_People", x => x.Id);
                    table.ForeignKey(
                        name: "FK_People_Parties_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "Parties",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "InformalOrganizations",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_InformalOrganizations", x => x.Id);
                    table.ForeignKey(
                        name: "FK_InformalOrganizations_Organizations_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "Organizations",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "LegalOrganizations",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    FederalTaxIdNumber = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_LegalOrganizations", x => x.Id);
                    table.ForeignKey(
                        name: "FK_LegalOrganizations_Organizations_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "Organizations",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Agents",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    Number = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                    License = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: true),
                    StaffCode = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: true),
                    ClientNumber = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: true),
                    AgentChannelId = table.Column<Guid>(type: "uuid", nullable: true),
                    AgentMasterId = table.Column<Guid>(type: "uuid", nullable: true),
                    ConfigCode = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    Description = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Agents", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Agents_AgentChannels_AgentChannelId",
                        column: x => x.AgentChannelId,
                        principalSchema: "parties",
                        principalTable: "AgentChannels",
                        principalColumn: "Id");
                    table.ForeignKey(
                        name: "FK_Agents_AgentMasters_AgentMasterId",
                        column: x => x.AgentMasterId,
                        principalSchema: "parties",
                        principalTable: "AgentMasters",
                        principalColumn: "Id");
                    table.ForeignKey(
                        name: "FK_Agents_PartyRoles_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "PartyRoles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "InsuredParties",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_InsuredParties", x => x.Id);
                    table.ForeignKey(
                        name: "FK_InsuredParties_PartyRoles_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "PartyRoles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Invoices",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Invoices", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Invoices_PartyRoles_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "PartyRoles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Insureds",
                schema: "parties",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Insureds", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Insureds_InsuredParties_Id",
                        column: x => x.Id,
                        principalSchema: "parties",
                        principalTable: "InsuredParties",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_AgentChannels_Code",
                schema: "parties",
                table: "AgentChannels",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_AgentMasters_Code",
                schema: "parties",
                table: "AgentMasters",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Agents_AgentChannelId",
                schema: "parties",
                table: "Agents",
                column: "AgentChannelId");

            migrationBuilder.CreateIndex(
                name: "IX_Agents_AgentMasterId",
                schema: "parties",
                table: "Agents",
                column: "AgentMasterId");

            migrationBuilder.CreateIndex(
                name: "IX_Agents_Name",
                schema: "parties",
                table: "Agents",
                column: "Name");

            migrationBuilder.CreateIndex(
                name: "IX_Agents_Number_ClientNumber",
                schema: "parties",
                table: "Agents",
                columns: new[] { "Number", "ClientNumber" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_AssetRoles_AssetId",
                schema: "parties",
                table: "AssetRoles",
                column: "AssetId");

            migrationBuilder.CreateIndex(
                name: "IX_AssetRoles_AssetRoleTypeId",
                schema: "parties",
                table: "AssetRoles",
                column: "AssetRoleTypeId");

            migrationBuilder.CreateIndex(
                name: "IX_AssetRoles_PartyId",
                schema: "parties",
                table: "AssetRoles",
                column: "PartyId");

            migrationBuilder.CreateIndex(
                name: "IX_AssetRoleTypes_Code",
                schema: "parties",
                table: "AssetRoleTypes",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Assets_Name",
                schema: "parties",
                table: "Assets",
                column: "Name");

            migrationBuilder.CreateIndex(
                name: "IX_ContactMechanisms_ContactMechanismTypeId",
                schema: "parties",
                table: "ContactMechanisms",
                column: "ContactMechanismTypeId");

            migrationBuilder.CreateIndex(
                name: "IX_ContactMechanismTypes_Code",
                schema: "parties",
                table: "ContactMechanismTypes",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_LegalOrganizations_FederalTaxIdNumber",
                schema: "parties",
                table: "LegalOrganizations",
                column: "FederalTaxIdNumber");

            migrationBuilder.CreateIndex(
                name: "IX_Nationalities_Code",
                schema: "parties",
                table: "Nationalities",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Organizations_Name",
                schema: "parties",
                table: "Organizations",
                column: "Name");

            migrationBuilder.CreateIndex(
                name: "IX_Organizations_Reference",
                schema: "parties",
                table: "Organizations",
                column: "Reference");

            migrationBuilder.CreateIndex(
                name: "IX_Parties_NationalityId",
                schema: "parties",
                table: "Parties",
                column: "NationalityId");

            migrationBuilder.CreateIndex(
                name: "IX_Parties_PartyTitleId",
                schema: "parties",
                table: "Parties",
                column: "PartyTitleId");

            migrationBuilder.CreateIndex(
                name: "IX_PartyContactMechanisms_ContactMechanismId",
                schema: "parties",
                table: "PartyContactMechanisms",
                column: "ContactMechanismId");

            migrationBuilder.CreateIndex(
                name: "IX_PartyContactMechanisms_PartyId",
                schema: "parties",
                table: "PartyContactMechanisms",
                column: "PartyId");

            migrationBuilder.CreateIndex(
                name: "IX_PartyRoles_PartyId_PartyRoleTypeId",
                schema: "parties",
                table: "PartyRoles",
                columns: new[] { "PartyId", "PartyRoleTypeId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_PartyRoles_PartyRoleTypeId",
                schema: "parties",
                table: "PartyRoles",
                column: "PartyRoleTypeId");

            migrationBuilder.CreateIndex(
                name: "IX_PartyRoleTypes_Code",
                schema: "parties",
                table: "PartyRoleTypes",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_PartyTitles_Code",
                schema: "parties",
                table: "PartyTitles",
                column: "Code",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_People_CardId",
                schema: "parties",
                table: "People",
                column: "CardId");

            migrationBuilder.CreateIndex(
                name: "IX_People_FirstName_LastName",
                schema: "parties",
                table: "People",
                columns: new[] { "FirstName", "LastName" });

            migrationBuilder.CreateIndex(
                name: "IX_PostalAddresses_Name",
                schema: "parties",
                table: "PostalAddresses",
                column: "Name");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Agents",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "AssetRoles",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "InformalOrganizations",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "Insureds",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "Invoices",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "LegalOrganizations",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "PartyContactMechanisms",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "People",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "PostalAddresses",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "Vehicles",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "AgentChannels",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "AgentMasters",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "AssetRoleTypes",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "InsuredParties",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "Organizations",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "ContactMechanisms",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "Assets",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "PartyRoles",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "ContactMechanismTypes",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "Parties",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "PartyRoleTypes",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "Nationalities",
                schema: "parties");

            migrationBuilder.DropTable(
                name: "PartyTitles",
                schema: "parties");
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Contracts.Extensions;

namespace Mti.OrderManagement.Contracts.Orders
{
    public sealed record MtiOriginalCsvValue
    {
        public uint ID { get; set; }
        public string? TransID { get; set; }
        public string? cSTATUS { get; set; }

        public string? SALEDATE { get; set; }

        // product
        public string? REFPOLICYTYPE { get; set; }
        public string? POLICYTYPE { get; set; }
        public string? CPRODUCTNAME { get; set; }
        public string? CCAMPAIGN { get; set; }
        public string? CPACKAGE { get; set; }
        public string? CWORKSHOP { get; set; }

        public string? NTOTALPREMIUM { get; set; }
        public string? NPREMIUM { get; set; }


        public string? REMARK { get; set; }
        public string? REFNOTICENO { get; set; }
        public string? REFDETAILNO { get; set; }
        public string? REF_QUOTATION { get; set; }
        public string? SOURCE { get; set; }
        public string? SYSTEM_ID { get; set; }
        public string? CSTATUSMESSAGE { get; set; }
        public string? cCustomerInfoNo { get; set; }

        public string? CLOANNUMBER { get; set; }

        // OWNER
        private string? _CTITLETEXT_OWNER;
        public string? CTITLETEXT_OWNER
        {
            get
            {
                return _CTITLETEXT_OWNER;
            }
            set
            {
                _CTITLETEXT_OWNER = value.CleanNull();
            }
        }

        private string? _CGIVENNAME_OWNER;
        public string? CGIVENNAME_OWNER
        {
            get
            {
                return _CGIVENNAME_OWNER;
            }
            set
            {
                _CGIVENNAME_OWNER = value.CleanNull();
            }
        }

        private string? _CSURNAME_OWNER;
        public string? CSURNAME_OWNER
        {
            get
            {
                return _CSURNAME_OWNER;
            }
            set
            {
                _CSURNAME_OWNER = value.CleanNull();
            }
        }

        private string? _FULLNAME_OWNER;
        public string? FULLNAME_OWNER
        {
            get
            {
                return _FULLNAME_OWNER;
            }
            set
            {
                _FULLNAME_OWNER = value.CleanNull();
            }
        }

        // INV
        private string? _CTITLETEXT_INV;
        public string? CTITLETEXT_INV
        {
            get
            {
                return _CTITLETEXT_INV;
            }
            set
            {
                _CTITLETEXT_INV = value.CleanNull();
            }
        }

        private string? _CGIVENNAME_INV;
        public string? CGIVENNAME_INV
        {
            get
            {
                return _CGIVENNAME_INV;
            }
            set
            {
                _CGIVENNAME_INV = value.CleanNull();
            }
        }

        private string? _CSURNAME_INV;
        public string? CSURNAME_INV
        {
            get
            {
                return _CSURNAME_INV;
            }
            set
            {
                _CSURNAME_INV = value.CleanNull();
            }
        }

        private string? _FULLNAME_INV;
        public string? FULLNAME_INV
        {
            get
            {
                return _FULLNAME_INV;
            }
            set
            {
                _FULLNAME_INV = value.CleanNull();
            }
        }

        public decimal SUMINSURE { get; set; }
        public string? Deduct { get; set; }

        public string? DamageLifePerPerson { get; set; }
        public string? DamageLifePerTime { get; set; }
        public string? DamageInsurePerTime { get; set; }
        public string? AccidentPerDriver { get; set; }
        public string? MedicalInsure { get; set; }
        public string? InsureDriver { get; set; }

        // Vehicles
        public string? CVEHCODE { get; set; }
        public string? BRANDNAME { get; set; }
        public string? MODELNAME { get; set; }
        public string? NYRMANUF { get; set; }
        public string? CREGNO { get; set; }
        public string? CENGINE { get; set; }
        public string? CCHASSIS { get; set; }
        public string? CREGPROVINCE { get; set; }
        public string? NCC { get; set; }
        public string? NSEAT { get; set; }
        public string? NWEIGHT { get; set; }
        public string? NTOANNAGE { get; set; }
        public string? NPASSENGER { get; set; }
        public string? cPayPlan { get; set; }
        public string? cCollateralNo { get; set; }
        public string? cCarColour { get; set; }

        // party id
        public string? CCARDID_OWNER { get; set; }
        public string? NBIRTHDATE_OWNER { get; set; }
        public string? CNATIONLITY_OWNER { get; set; }

        public string? CCARDID_INV { get; set; }
        public string? NBIRTHDATE_INV { get; set; }
        public string? CNATIONLITY_INV { get; set; }

        // mobile
        public string? CTELMOBILE1_OWNER { get; set; }
        public string? CTELMOBILE2_OWNER { get; set; }
        public string? CTELHOME_OWNER { get; set; }
        public string? CTELOFFICE_OWNER { get; set; }
        public string? CEMAIL_OWNER { get; set; }

        // address
        public string? CADDRNO_OWNER { get; set; }
        public string? CADDRMOO_OWNER { get; set; }
        public string? CADDRFLOOR_OWNER { get; set; }
        public string? CADDRROOM_OWNER { get; set; }
        public string? CADDRMOOBAN_OWNER { get; set; }
        public string? CADDRBUILDING_OWNER { get; set; }
        public string? CADDRSOI_OWNER { get; set; }
        public string? CADDRROAD_OWNER { get; set; }
        public string? CADDRTUMBOL_OWNER { get; set; }
        public string? CADDRAMPUR_OWNER { get; set; }
        public string? CADDRPROVINCE_OWNER { get; set; }
        public string? CADDRZIPCODE_OWNER { get; set; }

        public string? CADDRLINE1_OWNER { get; set; }
        public string? CADDRLINE2_OWNER { get; set; }
        public string? CADDRLINE3_OWNER { get; set; }
        public string? CADDRLINE4_OWNER { get; set; }

        // POLICIES
        private string? _POLICYNO;
        public string? POLICYNO
        {
            get
            {
                return _POLICYNO;
            }
            set
            {
                _POLICYNO = value.CleanNull();
            }
        }
        //OLD_POLICY
        private string? _OLD_POLICY;
        public string? OLD_POLICY
        {
            get
            {
                return _OLD_POLICY;
            }
            set
            {
                _OLD_POLICY = value.CleanNull();
            }
        }

        public string? NEFFECTIVEDATE { get; set; }
        public string? NEXPIRYDATE { get; set; }

        public string? cBranchID_INV { get; set; }
        public string? cBranchName_INV { get; set; }
    }
}

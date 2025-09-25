package routines.errorcountbymcc;

import java.util.HashMap;
import java.util.Map;

/**
 * Classe utilitária para mapear códigos MCC para suas descrições
 * Baseada nos códigos reais do dataset Kaggle
 */
public class MCCDescriptionMapper {

    // Mapa estático com códigos MCC e descrições
    private static final Map<String, String> MCC_DESCRIPTIONS = new HashMap<>();

    static {
        // Inicializar o mapa com todos os códigos MCC conhecidos
        MCC_DESCRIPTIONS.put("1711", "Heating, Plumbing, Air Conditioning Contractors");
        MCC_DESCRIPTIONS.put("3001", "Steel Products Manufacturing");
        MCC_DESCRIPTIONS.put("3005", "Miscellaneous Metal Fabrication");
        MCC_DESCRIPTIONS.put("3006", "Miscellaneous Fabricated Metal Products");
        MCC_DESCRIPTIONS.put("3007", "Coated and Laminated Products");
        MCC_DESCRIPTIONS.put("3008", "Steel Drums and Barrels");
        MCC_DESCRIPTIONS.put("3009", "Fabricated Structural Metal Products");
        MCC_DESCRIPTIONS.put("3058", "Tools, Parts, Supplies Manufacturing");
        MCC_DESCRIPTIONS.put("3066", "Miscellaneous Metals");
        MCC_DESCRIPTIONS.put("3075", "Bolt, Nut, Screw, Rivet Manufacturing");
        MCC_DESCRIPTIONS.put("3132", "Leather Goods");
        MCC_DESCRIPTIONS.put("3144", "Floor Covering Stores");
        MCC_DESCRIPTIONS.put("3174", "Upholstery and Drapery Stores");
        MCC_DESCRIPTIONS.put("3256", "Brick, Stone, and Related Materials");
        MCC_DESCRIPTIONS.put("3260", "Pottery and Ceramics");
        MCC_DESCRIPTIONS.put("3359", "Non-Ferrous Metal Foundries");
        MCC_DESCRIPTIONS.put("3387", "Electroplating, Plating, Polishing Services");
        MCC_DESCRIPTIONS.put("3389", "Non-Precious Metal Services");
        MCC_DESCRIPTIONS.put("3390", "Miscellaneous Metalwork");
        MCC_DESCRIPTIONS.put("3393", "Heat Treating Metal Services");
        MCC_DESCRIPTIONS.put("3395", "Welding Repair");
        MCC_DESCRIPTIONS.put("3405", "Ironwork");
        MCC_DESCRIPTIONS.put("3504", "Gardening Supplies");
        MCC_DESCRIPTIONS.put("3509", "Industrial Equipment and Supplies");
        MCC_DESCRIPTIONS.put("3596", "Miscellaneous Machinery and Parts Manufacturing");
        MCC_DESCRIPTIONS.put("3640", "Lighting, Fixtures, Electrical Supplies");
        MCC_DESCRIPTIONS.put("3684", "Semiconductors and Related Devices");
        MCC_DESCRIPTIONS.put("3722", "Passenger Railways");
        MCC_DESCRIPTIONS.put("3730", "Ship Chandlers");
        MCC_DESCRIPTIONS.put("3771", "Railroad Passenger Transport");
        MCC_DESCRIPTIONS.put("3775", "Railroad Freight");
        MCC_DESCRIPTIONS.put("3780", "Computer Network Services");
        MCC_DESCRIPTIONS.put("4111", "Local and Suburban Commuter Transportation");
        MCC_DESCRIPTIONS.put("4112", "Passenger Railways");
        MCC_DESCRIPTIONS.put("4121", "Taxicabs and Limousines");
        MCC_DESCRIPTIONS.put("4131", "Bus Lines");
        MCC_DESCRIPTIONS.put("4214", "Motor Freight Carriers and Trucking");
        MCC_DESCRIPTIONS.put("4411", "Cruise Lines");
        MCC_DESCRIPTIONS.put("4511", "Airlines");
        MCC_DESCRIPTIONS.put("4722", "Travel Agencies");
        MCC_DESCRIPTIONS.put("4784", "Tolls and Bridge Fees");
        MCC_DESCRIPTIONS.put("4814", "Telecommunication Services");
        MCC_DESCRIPTIONS.put("4829", "Money Transfer");
        MCC_DESCRIPTIONS.put("4899", "Cable, Satellite, and Other Pay Television Services");
        MCC_DESCRIPTIONS.put("4900", "Utilities - Electric, Gas, Water, Sanitary");
        MCC_DESCRIPTIONS.put("5045", "Computers, Computer Peripheral Equipment");
        MCC_DESCRIPTIONS.put("5094", "Precious Stones and Metals");
        MCC_DESCRIPTIONS.put("5192", "Books, Periodicals, Newspapers");
        MCC_DESCRIPTIONS.put("5193", "Florists Supplies, Nursery Stock and Flowers");
        MCC_DESCRIPTIONS.put("5211", "Lumber and Building Materials");
        MCC_DESCRIPTIONS.put("5251", "Hardware Stores");
        MCC_DESCRIPTIONS.put("5261", "Lawn and Garden Supply Stores");
        MCC_DESCRIPTIONS.put("5300", "Wholesale Clubs");
        MCC_DESCRIPTIONS.put("5310", "Discount Stores");
        MCC_DESCRIPTIONS.put("5311", "Department Stores");
        MCC_DESCRIPTIONS.put("5411", "Grocery Stores, Supermarkets");
        MCC_DESCRIPTIONS.put("5499", "Miscellaneous Food Stores");
        MCC_DESCRIPTIONS.put("5533", "Automotive Parts and Accessories Stores");
        MCC_DESCRIPTIONS.put("5541", "Service Stations");
        MCC_DESCRIPTIONS.put("5621", "Women's Ready-To-Wear Stores");
        MCC_DESCRIPTIONS.put("5651", "Family Clothing Stores");
        MCC_DESCRIPTIONS.put("5655", "Sports Apparel, Riding Apparel Stores");
        MCC_DESCRIPTIONS.put("5661", "Shoe Stores");
        MCC_DESCRIPTIONS.put("5712", "Furniture, Home Furnishings, and Equipment Stores");
        MCC_DESCRIPTIONS.put("5719", "Miscellaneous Home Furnishing Stores");
        MCC_DESCRIPTIONS.put("5722", "Household Appliance Stores");
        MCC_DESCRIPTIONS.put("5732", "Electronics Stores");
        MCC_DESCRIPTIONS.put("5733", "Music Stores - Musical Instruments");
        MCC_DESCRIPTIONS.put("5812", "Eating Places and Restaurants");
        MCC_DESCRIPTIONS.put("5813", "Drinking Places (Alcoholic Beverages)");
        MCC_DESCRIPTIONS.put("5814", "Fast Food Restaurants");
        MCC_DESCRIPTIONS.put("5815", "Digital Goods - Media, Books, Apps");
        MCC_DESCRIPTIONS.put("5816", "Digital Goods - Games");
        MCC_DESCRIPTIONS.put("5912", "Drug Stores and Pharmacies");
        MCC_DESCRIPTIONS.put("5921", "Package Stores, Beer, Wine, Liquor");
        MCC_DESCRIPTIONS.put("5932", "Antique Shops");
        MCC_DESCRIPTIONS.put("5941", "Sporting Goods Stores");
        MCC_DESCRIPTIONS.put("5942", "Book Stores");
        MCC_DESCRIPTIONS.put("5947", "Gift, Card, Novelty Stores");
        MCC_DESCRIPTIONS.put("5970", "Artist Supply Stores, Craft Shops");
        MCC_DESCRIPTIONS.put("5977", "Cosmetic Stores");
        MCC_DESCRIPTIONS.put("6300", "Insurance Sales, Underwriting");
        MCC_DESCRIPTIONS.put("7011", "Lodging - Hotels, Motels, Resorts");
        MCC_DESCRIPTIONS.put("7210", "Laundry Services");
        MCC_DESCRIPTIONS.put("7230", "Beauty and Barber Shops");
        MCC_DESCRIPTIONS.put("7276", "Tax Preparation Services");
        MCC_DESCRIPTIONS.put("7349", "Cleaning and Maintenance Services");
        MCC_DESCRIPTIONS.put("7393", "Detective Agencies, Security Services");
        MCC_DESCRIPTIONS.put("7531", "Automotive Body Repair Shops");
        MCC_DESCRIPTIONS.put("7538", "Automotive Service Shops");
        MCC_DESCRIPTIONS.put("7542", "Car Washes");
        MCC_DESCRIPTIONS.put("7549", "Towing Services");
        MCC_DESCRIPTIONS.put("7801", "Athletic Fields, Commercial Sports");
        MCC_DESCRIPTIONS.put("7802", "Recreational Sports, Clubs");
        MCC_DESCRIPTIONS.put("7832", "Motion Picture Theaters");
        MCC_DESCRIPTIONS.put("7922", "Theatrical Producers");
        MCC_DESCRIPTIONS.put("7995", "Betting (including Lottery Tickets, Casinos)");
        MCC_DESCRIPTIONS.put("7996", "Amusement Parks, Carnivals, Circuses");
        MCC_DESCRIPTIONS.put("8011", "Doctors, Physicians");
        MCC_DESCRIPTIONS.put("8021", "Dentists and Orthodontists");
        MCC_DESCRIPTIONS.put("8041", "Chiropractors");
        MCC_DESCRIPTIONS.put("8043", "Optometrists, Optical Goods and Eyeglasses");
        MCC_DESCRIPTIONS.put("8049", "Podiatrists");
        MCC_DESCRIPTIONS.put("8062", "Hospitals");
        MCC_DESCRIPTIONS.put("8099", "Medical Services");
        MCC_DESCRIPTIONS.put("8111", "Legal Services and Attorneys");
        MCC_DESCRIPTIONS.put("8931", "Accounting, Auditing, and Bookkeeping Services");
        MCC_DESCRIPTIONS.put("9402", "Postal Services - Government Only");
    }

    /**
     * Obtém a descrição para um código MCC
     * @param mccCode Código MCC
     * @return Descrição do MCC ou informação de código desconhecido
     */
    public static String getDescription(String mccCode) {
        if (mccCode == null || mccCode.trim().isEmpty()) {
            return "Unknown MCC Code";
        }

        String description = MCC_DESCRIPTIONS.get(mccCode.trim());
        if (description != null) {
            return description;
        } else {
            return "Unknown MCC Code: " + mccCode;
        }
    }

    /**
     * Verifica se o código MCC é conhecido
     * @param mccCode Código MCC
     * @return true se o código é conhecido
     */
    public static boolean isKnownMCC(String mccCode) {
        if (mccCode == null || mccCode.trim().isEmpty()) {
            return false;
        }
        return MCC_DESCRIPTIONS.containsKey(mccCode.trim());
    }

    /**
     * Obtém a categoria geral baseada no primeiro dígito do MCC
     * @param mccCode Código MCC
     * @return Categoria geral
     */
    public static String getGeneralCategory(String mccCode) {
        if (mccCode == null || mccCode.trim().isEmpty() || mccCode.length() < 1) {
            return "Unknown Category";
        }

        char firstDigit = mccCode.trim().charAt(0);
        switch (firstDigit) {
            case '1':
                return "Contracted Services";
            case '2':
                return "Airlines";
            case '3':
                return "Manufacturing/Industry";
            case '4':
                return "Transportation/Utilities";
            case '5':
                return "Retail/Wholesale";
            case '6':
                return "Financial Services";
            case '7':
                return "Business/Personal Services";
            case '8':
                return "Professional Services";
            case '9':
                return "Government Services";
            default:
                return "Other Category";
        }
    }

    /**
     * Analisa a possível causa de erros baseada no tipo de MCC
     * @param mccCode Código MCC
     * @return Análise da possível causa de erros
     */
    public static String analyzeErrorCause(String mccCode) {
        if (mccCode == null || mccCode.trim().isEmpty()) {
            return "Unknown error pattern";
        }

        String description = getDescription(mccCode);

        // Análise baseada em padrões conhecidos
        if (mccCode.equals("5541")) {
            return "Service stations: connectivity issues at fuel pumps";
        } else if (mccCode.equals("5411")) {
            return "Supermarkets: high volume transactions";
        } else if (mccCode.equals("4784")) {
            return "Toll systems: automated payment failures";
        } else if (mccCode.equals("4829")) {
            return "Money transfers: complex validation processes";
        } else if (mccCode.equals("5812") || mccCode.equals("5814")) {
            return "Restaurants: diverse POS systems, peak hour issues";
        } else if (mccCode.equals("4121")) {
            return "Taxis: mobile connectivity problems";
        } else if (mccCode.startsWith("49")) {
            return "Transportation/Utilities: infrastructure variability";
        } else if (mccCode.startsWith("58")) {
            return "Food services: peak demand, system diversity";
        } else if (mccCode.startsWith("54")) {
            return "Automotive: automated systems, connectivity";
        } else if (mccCode.startsWith("80")) {
            return "Medical services: strict validation requirements";
        } else if (mccCode.startsWith("75")) {
            return "Automotive services: mobile/field operations";
        } else {
            return "Standard transaction processing issues";
        }
    }
}
# Creator:      Sergii Butrymenko
# Dataset Name: #300 Washington Public Employee Salary2
# Task #:       104
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/382
# Dataset Link: https://lokic.locallabs.com/data_sets/300
# Created:      March 2023

# ruby mlc.rb --tool="clean::wa::washington_public_employee_salary2"
# ruby mlc.rb --tool="clean::wa::washington_public_employee_salary2" --mode='job_title'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    job_title: {
      raw_table: 'washington_public_employee_salary2',
      clean_table: 'washington_public_employee_salary2__position_clean',
      raw_column: 'position',
      clean_column: 'position_clean',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :job_title
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_titles(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #104] #300 Washington Public Employee Salary2* \n>#{type} #{message}",
    as_user: true
  )
end

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:clean_table]};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
    constraints = "UNIQUE (#{table_info[:raw_column]})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:state_column]
      state = "#{table_info[:state_column]} VARCHAR(2),"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{type}
         #{state}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
         # CHARACTER SET latin1 COLLATE latin1_swedish_ci;
    SQL
    #{local_connection}
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}".cyan
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND r.created_at >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  return if names_list.empty?

  parts = names_list.each_slice(10_000).to_a
  parts.each do |part|
    insert_query = <<~SQL
      INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
      VALUES
    SQL
    part.each do |item|
      insert_query << "('#{escape(item[table_info[:raw_column]])}','#{item['scrape_date']}'),"
    end
    insert_query = "#{insert_query.chop};"
    puts insert_query.red
    route.query(insert_query)
  end
end

def clean_titles(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  titles_list = route.query(query).to_a

  titles_list.each do |item|
    puts JSON.pretty_generate(item).yellow
    clean_title = item[table_info[:raw_column]].dup

    clean_title.gsub!(/\bGrowing Area Secretary Manager\b/i, 'Growing Area Security Manager')
    clean_title.gsub!(/\bImmunization Information System Secretary Manager\b/i, 'Immunization Information System Security Manager')
    clean_title.gsub!(/\bSctf Kc\b/i, 'Secure Continuous Treatment Facility King County')
    clean_title.gsub!(/\bYk Vly Scl\b/i, 'Yakima Valley School')
    clean_title.gsub!(/\bAssistant Chief Industrial Appeals Judge-Biia\b/i, 'Assistant Chief Industrial Appeals Judge-Board of Industrial Insurance Appeals')
    clean_title.gsub!(/\bA\/C IND APP JUDG\b/i, 'Assistant Chief Industry Application Judge')
    clean_title.gsub!(/\bA\/D FOR ADMIN SVCS\b/i, 'Assistant Director for Administrative Services')
    clean_title.gsub!(/\bA\/D LAB SVS\b/i, 'Assistant Director for Laboratory Services')
    clean_title.gsub!(/\bA\/D RET SRVCS\b/i, 'Assistant Director of Retirement Services')
    clean_title.gsub!(/\bA\/D WEB & COMM SVC\b/i, 'Assistant Director of Web and Communication Services')
    clean_title.gsub!(/\bA\/D, COMMUNITY SERVS & HOUSING DIV - COM\b/i, 'Assistant Director of Community Services and Housing Division - Commerce')
    clean_title.gsub!(/\bA\/D, LOCAL GOV & INFRA DIV - COMMERCE\b/i, 'Assistant Director of Local Government and Infrastructure Division - Commerce')
    clean_title.gsub!(/\bA\/S D\.O\.H\./i, 'Assistant Secretary for the Department of Health')
    clean_title.gsub!(/\bASST\/ASSOC DIRECTOR 2 - AAOD2\b/i, 'Assistant/Associate Director 2 - Assistant/Associate Director 2')
    clean_title.gsub!(/\bEDUCATIONAL PROGRAM DIRECTOR 2 - EDPD2\b/i, 'Educational Program Director 2 - Educational Program Director 2 ')
    clean_title.gsub!(/\bHR Manager-Biia\b/i, 'Human Resources Manager - Board of Industrial Insurance')
    clean_title.gsub!(/\bITRC\b/i, 'Information Technology Resource Center')
    clean_title.gsub!(/\bNatural Resource Camp Manager Cedar Crk\b/i, 'Natural Resource Camp Manager - Cedar Creek')
    clean_title.gsub!(/\bIesl\/Annual Contract\b/i, 'Intensive English as a Second Language (IESL) Program/Annual Contract')
    clean_title.gsub!(/\bAdministrator\/Exempt Special Contr\b/i, 'Administrator/Exempt Special Control')
    clean_title.gsub!(/\bContrctr\b/i, 'Contractor')
    clean_title.gsub!(/\bInsurance Communication\b/i, 'Insurance Commissioner')
    clean_title.gsub!(/\bDean Com & Social Scie\b/i, 'Dean of Communication and Social Sciences')
    clean_title.gsub!(/\bGamb Com Program Manager\b/i, 'Gambling Commission - Program Manager')
    clean_title.gsub!(/\bMnlight\b/i, 'Moonlight')
    clean_title.gsub!(/\bPHYSICIAN - PHYS0\b/i, 'Physician - Physician ')
    clean_title.gsub!(/\bRg3 Complnc Manager\b/i, 'Сompliance Manager')
    clean_title.gsub!(/\bSENIOR ENERGY POLICY SPECIALIST - COM\b/i, 'Senior Energy Policy Specialist Commerce')
    clean_title.gsub!(/\bSenior Compliance of\b/i, 'Senior Compliance of Housing Finance Commission')
    clean_title.gsub!(/\bA\/D, Community Servs & Housing Division Com\b/i, 'A/D, Community Services & Housing Division Commerce')
    clean_title.gsub!(/\bDEPUTY INSUR COM\b/i, 'Deputy Insurance Commissioner')
    clean_title.gsub!(/\bDirector Com Svs Division\b/i, 'Director Community Services Division')
    clean_title.gsub!(/\bDO - TFM CGAP MGR\b/i, 'Division Operations - Transportation Finance and Management Certified Grants Administrator Program Manager')
    clean_title.gsub!(/\bGU SOAR STEM COACH TEMPORARY\b/i, 'GEAR UP SOAR STEM Coach Temporary')
    clean_title.gsub!(/\bHEAD OF DEVELOPMENT 2 - DEVO2\b/i, 'Head of Development 2 - Head of Development 2')
    clean_title.gsub!(/\bWFSE\b/i, 'Washington Federation of State Employees')
    clean_title.gsub!(/\bFTF-TENURE( TRACK)?\b/i, 'Full-Time Faculty Tenure Track')
    clean_title.gsub!(/\bAC\b/i, 'Adult Correctional')
    clean_title.gsub!(/\bRELIEF AB\b/i, 'Relief - Able Bodied')
    clean_title.gsub!(/\bRELIEF CM\b/i, 'Relief - Congestion Mitigation')
    clean_title.gsub!(/\bINSTR&CLASSROOM SUPT TCH3\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bFDA SEIU - PROJECT\b/i, 'Food and Drug Administration - Service Employees International Union Project')
    clean_title.gsub!(/\bAPL-FIELD ENGINEER 2 (E S 9)\b/i, 'Applied Physics Laboratory - Field Engineer 2')
    clean_title.gsub!(/\bDCS DIST MANAGER\b/i, 'Division of Child Support - District Manager')
    clean_title.gsub!(/\bHAB - PROTECTION DM\b/i, 'Habitat - Protection DM')
    clean_title.gsub!(/\bPTF\/FTF-NON TEACH HOURLY\b/i, 'Part-Time Faculty/Full-Time Faculty - Non Teach Hourly')
    clean_title.gsub!(/\bPROCURE SUPPLY SPEC 3\b/i, 'Procurement & Supply Specialist 3')
    clean_title.gsub!(/\bPEB BENEFIT STRATEGY & DESIGN MANAGER\b/i, 'Public Employees Benefits - Benefit Strategy & Design Manager')
    clean_title.gsub!(/\bPE - DESN\b/i, 'Project Engineer Design')
    clean_title.gsub!(/\bPASRR PROG MGR\b/i, 'Preadmission Screening and Resident Review - Program Manager')
    clean_title.gsub!(/\bORG CHANGE MGR\b/i, 'Organizational Change Manager')
    clean_title.gsub!(/\bOCR DIR\b/i, 'Office of Columbia River Director')
    clean_title.gsub!(/\bV PROVOST UNDERGRAD ED - 10901\b/i, 'Vice Provost Undergrad Education 10901')
    clean_title.gsub!(/\bDIRECTOR - ENROLL\/FINC AID\b/i, 'Director of Enrollment Services and Financial Aid')
    clean_title.gsub!(/\bASST CHIEF INDUSTRIAL APPEALS JUDGE-BIIA\b/i, 'Assistant Chief Industrial Appeals Judge-Board of Industrial Insurance Appeals')
    clean_title.gsub!(/\bAST.DR OC S&H\b/i, 'Assistant Director Division of Occupational Safety & Health')
    clean_title.gsub!(/\bBILINGUAL INS&CLAS SUPTEC1\b/i, 'Bilingual Instruction & Classroom Support Technician 1')
    clean_title.gsub!(/\bBPD POLICY ADVISOR\b/i, 'Business and Professions Division Policy Advisor')
    clean_title.gsub!(/\bPUBLIC RECORDS OFF & ADM RULE COORD\b/i, 'Public Records Officer & Administrative Rules Coordinator')
    clean_title.gsub!(/\bPUBLIC RELATIONS\/COMMUNICATION COORDINATOR\b/i, 'Public Relations/Communications Coordinator')
    clean_title.gsub!(/\bCLINICAL ASSIST PROF-DENT PATH\b/i, 'Clinical Assistant Professor-Dental Pathway')
    clean_title.gsub!(/\bHR MANAGER-BIIA\b/i, 'Human Resources Manager - Board of Industrial Insurance  Appeals')
    clean_title.gsub!(/\bRG3 COMPLNC MGR\b/i, 'RG3 Compliance Manager')
    clean_title.gsub!(/\bDPTY ASST COMM\b/i, 'Deputy Assistant Commissioner')
    clean_title.gsub!(/\bDIR AG ADT P D S\b/i, 'Director of Agriculture and Adaptive Programs for Persons with Developmental Disabilities Services')
    clean_title.gsub!(/\bDIRECTOR, COMMUNICABLE DISEASE - EPI\b/i, 'Director, Communicable Disease Epidemiology ')
    clean_title.gsub!(/\bDIR-STUDENT FINANCIAL SRVS\b/i, 'Director, Student Financial Services')
    clean_title.gsub!(/\bDIR-PHILANTHROPY\b/i, 'Director of Philanthropy')
    clean_title.gsub!(/\bDIR-LIBRARY & MEDIA SERV\b/i, 'Director - Library and Media Services')
    clean_title.gsub!(/\bDIR-ICWRTC\b/i, 'Director - Idaho Child Welfare Research & Training Center ')
    clean_title.gsub!(/\bDIR-INSTITUTIONAL EFFECTIV\b/i, 'Director of Institutional Effectiveness')
    clean_title.gsub!(/\bDIRECTOR OF INST\.? RESEARCH\b/i, 'Director of Institutional Research')
    clean_title.gsub!(/\bDIR-GOVERNMENTAL RELATIONS\b/i, 'Director of Government Relations')
    clean_title.gsub!(/\bDIR-FIN AID SCHOLARSHIPS\b/i, 'Director of Financial Aid & Scholarships')
    clean_title.gsub!(/\bDIR-FACILITIES SERVICES\b/i, 'Director of Facilities Services')
    clean_title.gsub!(/\bD\/M S DV A&AS\b/i, 'Director Management Service Division Advisory & Assistance Services')
    clean_title.gsub!(/\bEMS04 CIO, IT SYSTEMS & SERVICES DES\b/i, 'EMS04 CIO, Information Technology Systems & Services - Department of Enterprise Services')
    clean_title.gsub!(/\bEX DIR - SBOH\b/i, 'Executive Director at Washington State Board of Health')
    clean_title.gsub!(/\bFINANCIAL AID COUNSELOR - SSCO1\b/i, 'Financial Aid Counselor - Student Service Counselor 1')
    clean_title.gsub!(/\bHRL DE\/MEDIA ASSISTANT\b/i, 'Housing and Residence Life Digital Experience/Media Assistant')
    clean_title.gsub!(/\bMGR, RPAU\/CHIEF RVW JUDGE BRD OF APPEALS\b/i, 'Manager, Rules and Policies Assistance Unit/Chief Review Judge Board of Appeals')
    clean_title.gsub!(/\bSOLA PROGRAM ADMINISTRATOR\b/i, 'State Operated Living Alternatives - Program Administrator')
    clean_title.gsub!(/\bUNC PROP AUD MGR\b/i, 'Unclaimed Property Audit Manager')
    clean_title.gsub!(/\bDIR NRS HOM SVS\b/i, 'Director of Nursing Home Services')
    clean_title.gsub!(/\bIL COORDINATOR-FRTC 502190\b/i, 'Independent Living Coordinator - Family Resource Training Center 502190')
    clean_title.gsub!(/\bIMAGING TECHNOLOGIST-MAG RES IMAGING (NE S SEIU 925 HCP\/LT)\b/i, 'Imaging Technologist-Magnetic Resonance Imaging (Non-Exempt S Service Employees International Union 925 Healthcare personnel/Long-Terminal)')
    clean_title.gsub!(/\bMANAGER HO\b/i, 'Manager Homeownership')
    clean_title.gsub!(/\bPRIDEFOSTER\/PREADOPTTRA-502139\b/i, 'PRIDE Model of Practice with Foster Parents/Pre-Adoptive Parent Training 502139')
    clean_title.gsub!(/\bPROGRAM DIRECTOR - KUOW (E S 9)\b/i, 'Program Director - KUOW (Public Radio) (E S 9)')
    clean_title.gsub!(/\bRPMMENTORTRAINER-ICWRTC \b/i, 'Residential Peer Mentoring Mentor Trainer - Idaho Child Welfare Research & Training Center ')
    clean_title.gsub!(/\bRPM MENTORTRNR-ICWRTC\b/i, 'Residential Peer Mentoring Mentor Trainer - Idaho Child Welfare Research & Training Center ')
    clean_title.gsub!(/\bRPM MENTOR TRAIN-ICWRTC \b/i, 'Residential Peer Mentoring Mentor Trainer - Idaho Child Welfare Research & Training Center ')
    clean_title.gsub!(/\bRPM MENTOR TRNR ICWRTC\b/i, 'Residential Peer Mentoring Mentor Trainer - Idaho Child Welfare Research & Training Center ')
    clean_title.gsub!(/\bRPMMENTORTRAINER-ICWRTC \b/i, 'Residential Peer Mentoring Mentor Trainer - Idaho Child Welfare Research & Training Center ')
    clean_title.gsub!(/\bRPM MENTOR TRAIN-ICWRTC\b/i, 'Residential Peer Mentoring Mentor Trainer - Idaho Child Welfare Research & Training Center')
    clean_title.gsub!(/\bSTUDENT SERVICE MANAGER 1 - SSMG1\b/i, 'Student Service Manager 1 - Student Service Manager 1')
    clean_title.gsub!(/\bSOLA PROGRAM MANAGER\b/i, 'Supporting Organization for Leprosy Affected Persons Program Manager')
    clean_title.gsub!(/\bUID LACEY CC ADJ MGR\b/i, 'Unemployment Insurance Division Lacey Call Center Adjudicator Manager')
    clean_title.gsub!(/\bWAIVER REQUIREMENTS & MICP PROG MGR\b/i, "Waiver Requirement & Medically Intensive Children's Program - Program Manager")
    clean_title.gsub!(/\bWMS 2-GSRO SCIENCE\b/i, "WMS2 - Governor's Salmon Recovery Office  Science")

    clean_title.gsub!(/\bAAA SPECIALIST\/PROG MGR\b/i, 'Area Agency on Aging Specialist/Program Manager')
    clean_title.gsub!(/\bACTIVITY ADV-ASST\/COACH\b/i, 'Activity Advisor-Assistant/Coach')

    clean_title.gsub!(/\bADM APPEALS JUDGE\b/i, 'Administrative Appeals Judge')
    clean_title.gsub!(/\bADM ASST INS COM\b/i, 'Administrative Assistant Insurance Commissioner')
    clean_title.gsub!(/\bADM FOREST ROADS SECTION MANAGER\b/i, 'Administrative Forest Roads Section Manager')
    clean_title.gsub!(/\bADM LAND SURVEY MANAGER\b/i, 'Administrative Land Survey Manager')
    clean_title.gsub!(/\bADM NATURAL HERITAGE\/AREAS\b/i, 'Administration Natural Heritage/Areas')
    clean_title.gsub!(/\bADM OPERATIONS\b/i, 'Administrative Operations')
    clean_title.gsub!(/\bADM ORCA STRAITS\b/i, 'Administration Orca Straits')
    clean_title.gsub!(/\bADM RIVERS DISTRICT\b/i, 'Administration Rivers District')
    clean_title.gsub!(/\bADM SHORELINE DISTRICT\b/i, 'Administration Shoreline District')
    clean_title.gsub!(/\bADM SILIVICULTURE\b/i, 'Administration Silviculture')
    clean_title.gsub!(/\bADM SURFACE MINE RECLAMATION\b/i, 'Administration Surface Mine Reclamation')
    clean_title.gsub!(/\bADM TITLE RECORDS OFFICE\b/i, 'Administration Title Records Office')
    clean_title.gsub!(/\bADM TRANSACTIONS MANAGER\b/i, 'Administrative Transactions Manager')

    clean_title.gsub!(/\bADMIN ASSISTANT DNR\b/i, 'Administrative Assistant - Department of Natural Resources')
    clean_title.gsub!(/\bADMIN ASST, A G\b/i, "Administrative Assistant, Attorney General's Office")
    clean_title.gsub!(/\bADMIN SERVICE ADMINISTRATR\b/i, 'Administration Service Administrator')
    clean_title.gsub!(/\bADMIN TEAM LEAD\b/i, 'Administration Team Lead')
    clean_title.gsub!(/\bADMIN VETERANS SVC\b/i, 'Administration Veterans Service')

    clean_title.gsub!(/\bBUS\. ADMIN\. FACULTY\b/i, 'Business Administration Faculty')
    clean_title.gsub!(/\bGRANT AND CONTRACT SPECIALIST\b/i, 'Grants and Contracts Specialist')
    clean_title.gsub!(/\bIT MANAGER 2 - ITMR2\b/i, 'Information Technology Manager 2 - ITMR2')
    clean_title.gsub!(/\bIT MANAGER 3 - ITMR3\b/i, 'Information Technology Manager 3 - ITMR3')
    clean_title.gsub!(/\bOILER,MARINE (NE S IBU)\b/i, 'Oiler, Marine (Non-Exempt S Ibu)')
    clean_title.gsub!(/\bOR\/ED ASSISTANT \(NE H SEIU 1199NW UWMC NORTHWEST SERVICE AND MAINTENANCE\)\b/i, 'OR/Education Assistant (NE H SEIU 1199NW UWMC Northwest Service and Maintenance)')
    clean_title.gsub!(/\bRAI COORD\/CASE MIX ACC REVIEW PROG MGR\b/i, 'RAI Coordinator/Case Mix Accuracy Review Program Manager')
    clean_title.gsub!(/\bSI WRKR OMBDM\b/i, 'Safety Inspector Worker Ombudsman')
    clean_title.gsub!(/\bSR INVEST COUNSEL\b/i, 'Senior Investigative Counsel')
    clean_title.gsub!(/\bTRUCK DRIVER 1-FLOAT SCH\b/i, 'Truck Driver 1-Float Schedule')
    clean_title.gsub!(/\bVP FOR COLLEGE ADV\b/i, 'Vice President for College Advancement')

    clean_title.gsub!(/\bAST\.DR OC S&H\b/i, 'Assistant Director Division of Occupational Safety & Health')
    clean_title.gsub!(/\bASSISTANT SUPERINTENDENT SP,SE,SI, &FA\b/i, 'Assistant Superintendent SP,SE,SI, &FA')
    clean_title.gsub!(/\bCONT & PURCH ADMINISTRATOR\b/i, 'Contracts & Purchasing Administrator')
    clean_title.gsub!(/\bCONTIN ED PT CONTRACT\b/i, 'Continuing Education Part-Time Contract')
    clean_title.gsub!(/\bCORR PROFESS-SPEC FUNDING\b/i, 'Correctional Professor-Special Funding')
    clean_title.gsub!(/\bCONSTRUCTION & MAINTENANCE SUPT (\d)\b/i, 'Construction & Maintenance Superintendent \1')
    clean_title.gsub!(/\bD\/DIR ECOL\b/i, 'Deputy Director Ecology')
    clean_title.gsub!(/\bD\/M S DV A&AS\b/i, 'Director Management Service Division Advisory & Assistance Services')
    clean_title.gsub!(/\bDATABASE\/WEB APP PROGRAMME\b/i, 'Database/Web Application Programmer')
    clean_title.gsub!(/\bDT SUP\b/i, 'Defensive Tactics Support')
    clean_title.gsub!(/\bDVR ASST ADMINISTRATOR\b/i, 'Division of Vocational Rehabilitation Assistant Administrator')
    clean_title.gsub!(/\bEXEC\. DIR, STR\.INIT &IN\.RE\b/i, 'Executive Director, Strategic Initiatives & Institutional Research')
    clean_title.gsub!(/\bFULL TIME FACULTY \b(\w{2})\b/i, 'Full-Time Faculty \1')
    # clean_title.gsub!(/\bFULL TIME FACULTY D2\b/i, 'Full Time Faculty D2')
    # clean_title.gsub!(/\bFULL TIME FACULTY F4\b/i, 'Full Time Faculty F4')
    # clean_title.gsub!(/\bFULL TIME FACULTY H4\b/i, 'Full Time Faculty H4')
    # clean_title.gsub!(/\bFULL TIME FACULTY HH\b/i, 'Full Time Faculty HH')
    # clean_title.gsub!(/\bFULL TIME FACULTY J2\b/i, 'Full Time Faculty J2')
    # clean_title.gsub!(/\bFULL TIME FACULTY L1\b/i, 'Full Time Faculty L1')
    # clean_title.gsub!(/\bFULL TIME FACULTY TT\b/i, 'Full Time Faculty TT')
    # clean_title.gsub!(/\bFULL TIME FACULTY ZZ\b/i, 'Full Time Faculty ZZ')
    clean_title.gsub!(/\bHIT PROJECT\/PROGRAM MANAGER\b/i, 'Health Information Technology Project/Program Manager')
    clean_title.gsub!(/\bHOMEOWNERWHIP DI\b/i, 'Homeownership Director')
    clean_title.gsub!(/\bHSA PGR MGR\b/i, 'Health Services Analysis - Program Manager')
    clean_title.gsub!(/\bIND PROV\/ADMIN HEARING PROG MGR\b/i, 'Individual Provider/Administrative Hearing Program Manager')
    clean_title.gsub!(/\bINST CLASSROOM SUP TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINST\/CLASS SUPPORT TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINST\/CLASSROOM SUP TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINST\/CLSRM SUPP TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTR & CLASS SUP TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTR\/CLASS SUPP TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTR&CLASSROOM SUPP TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTR%CLASSROON SUPT TCH4\b/i, 'Instruction & Classroom Support Technician 4')
    clean_title.gsub!(/\bINSTRCTN & CLSSRM SPPRT TCH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTRUC & CLASSRM SUPP TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTRUCT\/CLSRM SUPPORT TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTRUCTION & CLASSROOM SUPPORT TECH\b/i, 'Instruction & Classroom Support Technician')
    clean_title.gsub!(/\bINSTRUCTION\/CLASS\b/i, 'Instruction & Classroom')
    clean_title.gsub!(/\bLEAD SR PRJT MGR\b/i, 'Lead Senior Project Manager')
    clean_title.gsub!(/\bLECTURER FULL-TIME-COMPETITIVE RECRUIT\b/i, 'Lecturer Full-Time-Competitive Recruitment')
    clean_title.gsub!(/\bLIBRARY & ARCHIVES PARAPROF 3\b/i, 'Library & Archives Paraprofessional 3')
    clean_title.gsub!(/\bLOC HLTH SUPP SEC MGR\b/i, 'Level of Care Health Support Security Manager')
    clean_title.gsub!(/\bMICRO PRGM MGR\b/i, 'Microbiology Program Manager')
    clean_title.gsub!(/\bNUCLEAR MEDICINE PET\/CT TECHNOLOGIST (NE S SEIU 925 HCP\/LT)\b/i, 'Nuclear Medicine Positron Emission Tomography - Computed Tomography Technologist (Non-Exempt S Service Employees International Union 925 Healthcare personnel/Long-Terminal)')
    clean_title.gsub!(/\bOB TECHNOLOGIST (NE S SEIU 925 HCP\/LT)\b/i, 'Obstetric Technologists (Non-Exempt S Service Employees International Union 925 Healthcare personnel/Long-Terminal)')
    clean_title.gsub!(/\bPREV WG MGR\b/i, 'Prevailing Wage Manager')
    clean_title.gsub!(/\bPRIDEFOSTER\/PREADOPTTRA-502139\b/i, 'Pride Foster/Pre-Adoptive Parent Training - 502139')
    clean_title.gsub!(/\bPRT & IMAGING PLANT MGR\b/i, 'Print & Imaging Plant Manager')
    clean_title.gsub!(/\bREG ADMIN L&I\b/i, 'Regional Administrator Labor and Industries')
    clean_title.gsub!(/\bRHC PROGRAM MGR\b/i, 'Rural Health Clinic Program Manager')
    clean_title.gsub!(/\bPROC & SUPPLY SPEC 3\b/i, 'Procurement & Supply Specialist 3')
    clean_title.gsub!(/\bPRO-RATA-FT\b/i, 'Pro Rata Full-Time')
    clean_title.gsub!(/\bSD AND ESD ACCT, PRG SUPERVISOR\b/i, 'School Districts and Educational Service Districts Accounting, Program Supervisor')
    clean_title.gsub!(/\bSEMS OPERATIONS CHIEF\b/i, 'Standardized Emergency Management System Operations Chief')
    clean_title.gsub!(/\bSOLA PROG MGR\b/i, 'State Operated Living Alternatives Program Manager')
    clean_title.gsub!(/\bSOLA PROGRAM MANAGER\b/i, 'State Operated Living Alternatives Program Manager')
    clean_title.gsub!(/\bSPEC, GRAPHIC DESIGN\/ILL\b/i, 'Specialist, Graphic Design/Illustrator')
    clean_title.gsub!(/\bSPECIAL ASSIGNMENT-STH\b/i, 'Special Assignment-Substitute Teacher')
    clean_title.gsub!(/\bSPECIAL ASSIGN-SUB TEACHER\b/i, 'Special Assignment-Substitute Teacher')
    clean_title.gsub!(/\bSR COMPLIANCE OF\b/i, 'Senior Compliance Officer')
    clean_title.gsub!(/\bSUPERINTENDENT A - WCC\b/i, 'Superintendent A Washington Corrections Center')
    clean_title.gsub!(/\bTAX REFEREE-TAB\b/i, 'Tax Referee - Tax Appeals Board')
    clean_title.gsub!(/\bTEST CENTER ASST\b/i, 'Testing Center Assistant')
    clean_title.gsub!(/\bTSG CONTRACT EMPLOYEE\b/i, 'Technology Solutions Group Contract Employee')
    clean_title.gsub!(/\bTUTOR-NON STUDENT\b/i, 'Tutor Non-Student')
    clean_title.gsub!(/\bUID LACEY CC ADJ MGR\b/i, 'Unemployment Insurance Division Lacey Claims Center Adjudication Manager')
    clean_title.gsub!(/\bVCC PROG MGR\b/i, 'Veterans Conservation Corps Program Manager')
    clean_title.gsub!(/\bVICE PRES - STUDENT AFFAIR\b/i, 'Vice President - Student Affairs')
    clean_title.gsub!(/\bVICE PRES-ADMIN SRVICES\b/i, 'Vice President - Administrative Services')
    clean_title.gsub!(/\bVICE PRES-INSTR & STU SERV\b/i, 'Vice President of Instruction & Student Services')
    clean_title.gsub!(/\bVISIT FACULTY NATURAL SCI\b/i, 'Visiting Faculty Natural Science')
    clean_title.gsub!(/\bVISIT FACULTY SOCIAL SCI\b/i, 'Visiting Faculty Social Science')
    clean_title.gsub!(/\bWB2 - PA COMM AND SOCIAL MEDIA MANAGER\b/i, 'Wb2 Public Affairs Communication and Social Media Manager')
    clean_title.gsub!(/\bWCC CREW SUPERVISOR 2\b/i, 'Washington Conservation Corps Crew Supervisor 2')
    clean_title.gsub!(/\bWORKFORCE TRANS SPECIALIST\b/i, 'Workforce Transitions Specialist')
    clean_title.gsub!(/\bWS EVERETT ADMIN\b/i, 'WorkSource Everett Administrator')
    clean_title.gsub!(/\bWS YAKIMA ADMIN\b/i, 'WorkSource Yakima Administrator')
    clean_title.gsub!(/\bWSP ASSISTANT CHIEF\b/i, 'Washington State Patrol Assistant Chief')
    clean_title.gsub!(/\bWSP SERGEANT PAY GRADE\b/i, 'Washington State Patrol Sergeant Pay Grade')
    clean_title.gsub!(/\bWSP TROOPER PAY GRADE\b/i, 'Washington State Patrol Trooper Pay Grade')

    clean_title.gsub!(/\bALT STAFF CHIEF\b/i, 'ALT Staff Chief')
    clean_title.gsub!(/\bDIR ENROLL SVCS\/REGISTRAR\b/i, 'Director Enrollment Services/Registrar')
    clean_title.gsub!(/\bHRY LIB\/ARCHIVE PARAPROF'L\b/i, 'Hourly Library/Archive Paraprofessional')
    clean_title.gsub!(/\bCOMPL MGR REG 6\b/i, 'Compliance Manager Region 6')
    clean_title.gsub!(/\bDEAN FOR CORP\/CONT\. EDUC\./i, 'Dean for Corporate and Continuing Education')
    clean_title.gsub!(/\bDEAN OF PROF\/TECH ED\b/i, 'Dean of Professional/Technical Education')
    clean_title.gsub!(/\bDEV DIS REG ADM\b/i, 'Developmental Disabilities Regional Administrator')
    clean_title.gsub!(/\bDIR COM SVS DIV\b/i, 'Director Community Services Division')
    clean_title.gsub!(/\bDIR ELEARNING FOR EDUCATORS\b/i, 'Director E-Learning for Educators')
    clean_title.gsub!(/\bDIR MULTIFAMILY HOUSING & COM FACILITIES\b/i, 'Director Multifamily Housing and Community Facilities')
    clean_title.gsub!(/\bEC DIR\b/i, 'Employment Connections Director ')
    clean_title.gsub!(/\bEC PROGRAM SPECIALIST 2\b/i, 'Early Childhood Specialist 2')
    clean_title.gsub!(/\bEC REV FORCST SP\b/i, 'Economic and Revenue Forecast Specialist')
    clean_title.gsub!(/\bMAINT & OPS SUPT & REG WIDE SPEC MNT CRW\b/i, 'Maintenance & Operations Support & Regionwide Specialist Maintenance Crew')
    clean_title.gsub!(/\bADM HCP & SCI CONSULTATION\b/i, 'Administrative Habitat Conservation Planning & Scientific Consultation')
    clean_title.gsub!(/\bHRY TELCOM OPERATOR\b/i, 'Hourly Telecom Operator')

    clean_title.gsub!(/\bCHIEF, REG 2 BUSINESS CENTER\b/i, 'Chief, Region 2 Business Center')
    clean_title.gsub!(/\bDIR-POST AWARD\b/i, 'Director Post-Award')
    clean_title.gsub!(/\bFIELD SVCS PSYCH\b/i, 'Field Services Psychiatrist')
    clean_title.gsub!(/\bE0406 A\/D PRP TX\b/i, 'E0406 Assistant Director Property Tax')
    clean_title.gsub!(/\bE0409 A\/D TAA\b/i, 'E0409 Assistant Director Trade Adjustment Assistance')
    clean_title.gsub!(/\bCOM&EMPL SERV PG SPEC LEAD\b/i, 'Community&Employment Services Program Specialist Lead')
    clean_title.gsub!(/\bCOMM & EMPL SERV EMPL SPEC\b/i, 'Community&Employment Services Employment Specialist')

    clean_title.gsub!(/\bAD ACAD ADV\/CAREER SERVICE\b/i, 'Associate Director Academic Advising & Career Services')
    clean_title.gsub!(/\bAD AUTOMATION MIGRATION SPECIALIST\b/i, 'Active Directory Automation Migration Specialist')
    clean_title.gsub!(/\bAD CAMP\b/i, 'Assistant Director, Capital and Asset Management Program')
    clean_title.gsub!(/\bAD CHIEF\b/i, 'Assistant Director - Chief')
    clean_title.gsub!(/\bAD CONSUMER PROTECTION\b/i, 'Assistant Director Consumer Protection')
    clean_title.gsub!(/\bAD DATA QUALITY VISUALIZATION RESEARCH\b/i, 'Associate Director Data Quality Visualization Research')
    clean_title.gsub!(/\bAD DVLPMNT, LDRSHP ANN GIVING - MAGO1\b/i, 'Assistant Director Development, Leadership Annual Giving - Major Gifts Officer 1')
    clean_title.gsub!(/\bAD ENERGY\b/i, 'Assistant Director Energy')
    clean_title.gsub!(/\bAD FEDERATION SPECIALIST\b/i, 'Active Directory Federation Specialist (ADFS)')
    clean_title.gsub!(/\bAD FINANCIAL SERV\b/i, 'Assistant Director Financial Services')
    clean_title.gsub!(/\bAD FISH\b/i, 'Assistant Director Fish')
    clean_title.gsub!(/\bAD HABITAT\b/i, 'Assistant Director Habitat')
    clean_title.gsub!(/\bAD INTERCULTURAL SSERVICES\b/i, 'Associate Director Intercultural Services')
    clean_title.gsub!(/\bAD INTL PROG MKTG\/RECRUIT\b/i, 'Associate Director International Program Marketing & Recruitment')
    clean_title.gsub!(/\bAD LEGISLATION AND POLICY\b/i, 'Assistant Director Legislation and Policy')
    clean_title.gsub!(/\bAD NATNL CYBER SEC CENTERS\b/i, 'Associate Director National Cybersecurity Centers')
    clean_title.gsub!(/\bAD OF TITLE IX AND EEO\b/i, 'Associate Director of Title 9 and EEO')
    clean_title.gsub!(/\bAD RESEARCH & FISCAL ANALYSIS\b/i, 'Assistant Director Research & Fiscal Analysis')
    clean_title.gsub!(/\bAD SCIENCE AND RESEARCH\b/i, 'Assistant Director Science and Research')
    clean_title.gsub!(/\bAD SURFACE MINE RECLAMATION\b/i, 'Assistant Director Surface Mine Reclamation')
    clean_title.gsub!(/\bAD TELCOMM\b/i, 'Assistant Director Telecommunications')
    clean_title.gsub!(/\bAD TRANS\. SAFETY\b/i, 'Assistant Director Transportation Safety')
    clean_title.gsub!(/\bAD WILDLIFE\b/i, 'Assistant Director Wildlife')
    clean_title.gsub!(/\bAD, CONSERVATION & ENERGY PLANNING\b/i, 'Assistant Director, Conservation & Energy Planning')
    clean_title.gsub!(/\bAD, LICENSING & OVERSIGHT\b/i, 'Assistant Director, Licensing & Oversight')
    clean_title.gsub!(/\bAD, TECH SERVICES & FACILITIES PLANNING\b/i, 'Assistant Director, Technology Services & Facilities Planning')
    clean_title.gsub!(/\bAD, WATER, SOLID WASTE AND TRANS\./i, 'Assistant Director, Water, Solid Waste and Transportation')
    clean_title.gsub!(/\bAD-FOR ENTRY SERVICES\b/i, 'Associate Director - For Entry Services')
    clean_title.gsub!(/\bAD-STU ACCESS\/CAREER PW\b/i, 'Associate Director - Student Access/Career Pathways')
    clean_title.gsub!(/\bAD\/EX MOONLIGHT APPT\b/i, 'Assistant Director/Executive Moonlight Appointment')
    clean_title.gsub!(/\bAD\/HEAD MENS BB COACH\b/i, "Athletic Director/Head Men's Basketball Coach")
    clean_title.gsub!(/\bAG\/ADLT SV RG AD R(\d{2})\b/i, 'Aging & Adult Services Regional Administrator R\1')
    # clean_title.gsub!(/\bAG\/ADLT SV RG AD R02\b/i, 'Aging & Adult Services Regional Administrator R02')
    # clean_title.gsub!(/\bAG\/ADLT SV RG AD R03\b/i, 'Aging & Adult Services Regional Administrator R03')
    clean_title.gsub!(/\bASOC DEAN OF COUNSELING,AD\b/i, 'Associate Dean of Counseling, Advising')
    clean_title.gsub!(/\bASOC DIR AD & TRANS PLAN\b/i, 'Associate Director Advertising & Transportation Planning')
    clean_title.gsub!(/\bASSOC AD-ATHLETIC COMPLIANCE\b/i, 'Associate Athletic Director - Athletic Compliance')
    clean_title.gsub!(/\bASST AD BUSINESS & FINANCE\b/i, 'Assistant Athletic Director Business & Finance')
    clean_title.gsub!(/\bASST AD COMPLIANCE\/ACADEMICS - ADSS1\b/i, 'Assistant Athletic Director Compliance/Academics Assistant Director-Student Services')
    clean_title.gsub!(/\bASST AD-ATHLETIC COMPLIANCE\/SS\b/i, 'Associate Athletic Director - Athletic Compliance/SS')
    clean_title.gsub!(/\bASST AD-ATHLETIC PERFORMANCE\b/i, 'Associate Athletic Director - Athletic Performance')
    clean_title.gsub!(/\bASST AD-MARKETING\/CREATIVE SRV\b/i, 'Assistant Athletic Director for Marketing/Creative Services')
    clean_title.gsub!(/\bASST AD-MEDIA RELATIONS\b/i, 'Associate Athletic Director - Media Relations')
    clean_title.gsub!(/\bASST AD-TRAINING SERVICES\b/i, 'Assistant Athletic Director - Training Services')
    clean_title.gsub!(/\bASST DN PE\/ASST AD\b/i, 'Assistant Dean of Physical Education/Assistant Athletic Director')
    clean_title.gsub!(/\bCHIEF TECH OFFICER\b/i, 'Chief Technology Officer')
    clean_title.gsub!(/\bCOMM ASST-SESSION\b/i, 'Committee Assistant Session')
    clean_title.gsub!(/\bCOMM CLERK-SCS \(SESSION\)\b/i, 'Committee Clerk-Senate Committee Services (Session)')
    clean_title.gsub!(/\bCONFIDENTIAL ADMIN ASST\b/i, 'Confidential Administrative Assistant')
    clean_title.gsub!(/\bDEPUTY AD ENERGY\b/i, 'Deputy Assistant Director Energy')
    clean_title.gsub!(/\bDEPUTY AD, RATES\/TARIFFS\b/i, 'Deputy Assistant Director, Rates/Tariffs')
    clean_title.gsub!(/\bDEPUTY AD-DEVELOP\/REVENUE GEN\b/i, 'Deputy Athletic Director - Development/Revenue General')
    clean_title.gsub!(/\bDEPUTY AD-INTERNAL OPS\/SWA\b/i, 'Deputy Athletic Director - External Operations/Senior Woman Administrator')
    clean_title.gsub!(/\bEASTERN DW REG OPS MGR\b/i, 'Eastern Drinking Water Regional Operations Manager')
    clean_title.gsub!(/\bEMS04 AD, CONTRACTS & LEGAL DES\b/i, 'EMS04 Assistant Director, Contracts & Legal Department of Enterprise Services')
    clean_title.gsub!(/\bEMS04 AD, DES SERVICES\b/i, 'EMS04 Assistant Director, Department of Enterprise Services Services')
    clean_title.gsub!(/\bEMS04 AD, FACILITIES DIVISION DES\b/i, 'EMS04 Assistant Director, Facilities Division Department of Enterprise Services')
    clean_title.gsub!(/\bEMS04 AD, REAL ESTATE SERVICES\b/i, 'EMS04 Assistant Director, Real Estate Services')
    clean_title.gsub!(/\bINTER-CULTURAL CTR LEAD AD\b/i, 'Inter-Cultural Center Leadership Advisor')
    clean_title.gsub!(/\bLEARNING RESOURCE CTR - AD\b/i, 'Learning Resource Center - Administrator')
    clean_title.gsub!(/\bMENTORSHIP PRO LEADERSP AD\b/i, 'Mentorship Programs Leadership Advisor')
    clean_title.gsub!(/\bOEDC - DEPUTY AD\b/i, 'Office of Economic Development & Competitiveness - Deputy Assistant Director')
    clean_title.gsub!(/\bSENIOR ASSOCIATE AD\b/i, 'Senior Associate Athletic Director')
    clean_title.gsub!(/\bSPECIAL ASSIGNMENT-AD\b/i, 'Special Assignment - Associate Director')
    clean_title.gsub!(/\bSR AD TAX POLICY\b/i, 'Senior Assistant Director Tax Policy')

    clean_title.gsub!(/\bSSC REG TECH ASST PROGRAM SUP\b/i, 'SSC Reg Tech Assistant Program Support')
    clean_title.gsub!(/\bREG (\d) - REGION HATCHERY REFORM \/ OPS MGR\b/i, 'Region \1 Regional Hatchery Reform / Operations Manager')
    clean_title.gsub!(/\bJUV REH REG ADM R(\d{,2})\b/i, 'Juvenile Rehabilitation Regional Administrator Region \1')
    clean_title.gsub!(/\bREG DIR, DSHS R(\d{,2})\b/i, 'Regional Director, Department of Social and Health Services Region \1')
    clean_title.gsub!(/\bREGION HATCHERY REFORM \/ OPS MGR - REG (\d)\b/i, 'Regional Hatchery Reform / Operations Manager Region \1')
    clean_title.gsub!(/\bESA CSD R(\d{,2}) REG ADMINISTRATOR\b/i, 'ESA CSD Region \1 Regional Administrator')

    clean_title.gsub!(/\bMARIJUANA EDU, LIC AND REG SYS MGR\b/i, 'Marijuana Education, Licensed and Regulation System Manager')
    clean_title.gsub!(/\bFISH PLAN REG AND ESA RESPONSE MGR\b/i, 'Fishery Planning, Regulations, and ESA Response Manager')
    clean_title.gsub!(/\bCOORDINATOR-ADMIN OPERTNS\b/i, 'Coordinator-Administrative Operations')
    clean_title.gsub!(/\bCORRECTIONAL INDUSTRIES SUPV (\d), CORR\b/i, 'Correctional Industries Supervisor \1, Corrections')
    clean_title.gsub!(/\bDEPUTY TREAS\b/i, 'Deputy Treasurer')
    clean_title.gsub!(/\bDIR COMP\/INFO SUP SRV\b/i, 'Director of Computer and Information Support Services')
    clean_title.gsub!(/\bDIRECTOR FACILITIES & OPER\b/i, 'Director Facilities & Operations')
    clean_title.gsub!(/\bREG PRJ COORD\b/i, 'Regulatory Project Coordinator')

    clean_title.gsub!(/\bDEAN ENROLL SVS\/REG\/FINAID\b/i, 'Dean Enrollment Services/Registration/Financial Aid')
    clean_title.gsub!(/\bDEAN COM & SOC SCIE\b/i, 'Dean of Communication and Social Sciences')
    clean_title.gsub!(/\bMODE 5 - HS21 TRNS & REG\b/i, 'Mode 5 - HS21 Trainings & Registration')
    clean_title.gsub!(/\bREG\/ENROLLMENT CLERK ([IV]{1,3})\b/i, 'Registration/Enrollment Clerk \1')

    clean_title.gsub!(/\bADMISSIONS\/REG ASST ([IV]{1,3})\b/i, 'Admissions/Registrar Assistant \1')
    clean_title.gsub!(/\bDIR-TRANSFER POL\/ASSOC REG\b/i, 'Director-Transfer Policy/Associate Registrar')

    clean_title.gsub!(/\bENVIRONMENTAL PROGRAM MGR, OLYMPIC REG\b/i, 'Environmental Program Manager, Olympic Region')
    clean_title.gsub!(/\bASST REG ENVIRONMENTAL & HYDRAULIC MGR\b/i, 'Assistant Region Environmental & Hydraulic Manager')
    clean_title.gsub!(/\bASST REG TRAFFIC ENG\b/i, 'Assistant Region Traffic Engineer')
    clean_title.gsub!(/\bASST REG TRAFFIC ENG-DESIGN & SAFETY MGT\b/i, 'Assistant Region Traffic Engineer - Design & Safety Management')
    clean_title.gsub!(/\bMAINT & OPS SUPT & REG WIDE SPEC MNT CRW\b/i, 'Maintenance & Operations Support & Regionwide Specialist Maintenance Crew')
    clean_title.gsub!(/\bNAT RES REG MGR\b/i, 'Natural Resources Region Manager')

    # clean_title.gsub!(/\bV PRES BUSINESS FINANCIAL AFF - 10051\b/i, 'Vice President Business Financial Affairs 10051')
    clean_title.gsub!(/^DIR-ORGANIZATIONAL DEVELOPMENT\b(.+)/i, 'Director of Organizational Development\1')
    clean_title.gsub!(/^DIR-PUBLIC SAFETY\/POLICE CHIEF\b(.+)/i, 'Director of Public Safety/Police Chief\1')
    clean_title.gsub!(/^DIRECTOR, SECURITY & LICENSING\b(.+)/i, 'Director of Security & Licensing\1')
    clean_title.gsub!(/^DIRECTOR-PROGRAM OPERATIONS \(E S 11\)\b(.+)/i, 'Director of Program Operations (E S 11)\1')
    clean_title.gsub!(/^DIR\. REG\. SVS\.(.*)/i, 'Director of Regulatory Services\1')
    clean_title.gsub!(/^DIR, FISCAL SERVICES\b(.+)/i, 'Director of Fiscal Services\1')
    clean_title.gsub!(/^EXEC DIR MCINTYRE HALL\b(.+)/i, 'Executive Director of McIntyre Hall\1')
    clean_title.gsub!(/^EXEC\.DIR, INTNL\.EDU\b(.+)/i, 'Executive Director of International Education\1')
    clean_title.gsub!(/^V\.P\. ADMINISTRATIVE SRVS\b(.+)/i, 'Vice President of Administrative Services\1')
    clean_title.gsub!(/^VICE PRES-ADMIN SRVICES\b(.+)/i, 'Vice President of Administrative Services\1')
    clean_title.gsub!(/^VP ADMINISTRATIVE SERVICES\b(.+)/i, 'Vice President of Administrative Services\1')

    clean_title.gsub!(/\bAdmin Ass(istan)?t\b/i, 'Administrative Assistant')

    clean_title.gsub!(/\bClass\s?&\s?Comp\b/i, 'Classification & Compensation')
    clean_title.gsub!(/\bCT\sTechnologist\b/i, 'Computed Tomography Technologist')
    clean_title.gsub!(/\bFloat\sSch\b/i, 'Float Schedule')
    clean_title.gsub!(/\bInstr(uct?|ctn)?\s?[&\/]\s?Class(room|rm)?\b/i, 'Instruction & Classroom')
    clean_title.gsub!(/\bCtc\s?Link\b/i, 'CtcLink')
    clean_title.gsub!(/\bImmuniz\b/i, 'Immunizations')
    clean_title.gsub!(/\bInvest\sCounsel\b/i, 'Investigative Counsel')
    clean_title.gsub!(/\bMt\sBaker\b/i, 'Mt. Baker')
    clean_title.gsub!(/\bTele?com\b/i, 'Telecommunications')
    clean_title.gsub!(/\bWk\sFirst\b/i, 'WorkFirst')
    clean_title.gsub!(/\bWrkr\sOmbdm\b/i, 'Worker Ombudsman')

    clean_title.gsub!(/\bAFH\b/i, 'Adult Family Homes ')
    clean_title.gsub!(/\bAHCC\b/i, 'Airway Heights Corrections Center')
    clean_title.gsub!(/\bAIM\b/i, 'Analytics, Interoperability and Measurement')
    clean_title.gsub!(/\bAPL\b/i, 'Applied Physics Laboratory')
    clean_title.gsub!(/\bBBCC\b/i, 'Big Bend Community College')
    clean_title.gsub!(/\bCC\b/i, 'Claims Center')
    clean_title.gsub!(/\bCE\b/i, 'Community Education')
    clean_title.gsub!(/\bCEDAR CRK\b/i, 'Cedar Creek')
    clean_title.gsub!(/\bCFCO\b/i, 'Community First Choice Option')
    clean_title.gsub!(/\bCFMO\b/i, 'Construction and Facilities Management Office')
    clean_title.gsub!(/\bCI\b/i, 'Correctional Industries')
    clean_title.gsub!(/\bCIR\b/i, 'Critical Incident Review')
    clean_title.gsub!(/\bCRCC\b/i, 'Coyote Ridge Corrections Center')
    clean_title.gsub!(/\bDCS\b/i, 'Division of Child Support')
    clean_title.gsub!(/\bDDA\b/i, 'Developmental Disability Administrator')
    clean_title.gsub!(/\bDES\b/i, 'Department of Enterprise Services')
    clean_title.gsub!(/\bDM\b/i, 'District Manager')
    clean_title.gsub!(/\bEA\b/i, 'Eastern') if clean_title.match?(/\bregion\b/i)
    clean_title.gsub!(/\bEDPS1\b/i, 'Educational Specialist 1')
    clean_title.gsub!(/\bELI\b/i, 'English Language Institute')
    clean_title.gsub!(/\bES\b/i, 'Employment Security Department')
    clean_title.gsub!(/\bFDA\b/i, 'Food and Drug Administration')
    clean_title.gsub!(/\bGMHB\b/i, 'Growth Management Hearings Board')
    clean_title.gsub!(/\bGRCC\b/i, 'Grand Rapids Community College')
    clean_title.gsub!(/\bHEOC\b/i, 'Health Occupations')
    clean_title.gsub!(/\bHMC\b/i, 'Harborview Medical Center')
    clean_title.gsub!(/\bICWRT\b/i, 'Idaho Child Welfare Research and Training Center')
    clean_title.gsub!(/\bIHME\b/i, 'Institute for Health Metrics and Evaluation')
    clean_title.gsub!(/\bKUOW\b/i, 'KUOW (public radio)')
    clean_title.gsub!(/\bMARC\b/i, 'Mathematics Resource Center')
    clean_title.gsub!(/\bNWR\b/i, 'Northwest Region')
    clean_title.gsub!(/\bORIA\b/i, 'Office of Refugee and Immigrant Assistance')
    clean_title.gsub!(/\bPL-RS\b/i, 'Planner-Retrospective Salary')
    clean_title.gsub!(/\bRCW\b/i, 'Revised Code of Washington')
    clean_title.gsub!(/\bRTF\b/i, 'Renton Technical College')
    clean_title.gsub!(/\bSEC (AGENCY)\b/i, 'Securities and Exchange Commission')
    clean_title.gsub!(/\bSEIU\b/i, 'Service Employees International Union')
    # clean_title.gsub!(/\bSOAR\b/i, 'Success, Opportunity, Affordability and Rigor, Relevance and Relationships')
    clean_title.gsub!(/\bSOLA\b/i, 'State Operated Living Alternatives')
    clean_title.gsub!(/\bSpscc\b/i, 'South Puget Sound Community College')
    clean_title.gsub!(/\bSSMG1\b/i, 'Student Service Manager 1')
    clean_title.gsub!(/\bTsg\b/i, 'Technology Services Group')
    clean_title.gsub!(/\bTRFC\b/i, 'Transportation Revenue Forecast Council')
    clean_title.gsub!(/\bTSG\b/i, 'Technology Solutions Group')
    clean_title.gsub!(/\bUWMC\b/i, 'University of Washington Medical Center') unless clean_title == 'OR/Education Assistant (NE H SEIU 1199NW UWMC Northwest Service and Maintenance)'
    clean_title.gsub!(/\bUWMC-ML\b/i, 'University of Washington Medical Center - Montlake')
    clean_title.gsub!(/\bVCC\b/i, 'Veterans Conservation Corps')
    clean_title.gsub!(/\bWA\b/i, 'Washington')
    clean_title.gsub!(/\bWAC\b/i, 'Washington Administrative Code')
    # clean_title.gsub!(/\bWCC\b/i, 'Washington Corrections Center')
    clean_title.gsub!(/\bWCC\b/i, 'Washington Conservation Corps')
    clean_title.gsub!(/\bWETRC\b/i, 'Washington Environment Training Center')
    clean_title.gsub!(/\bWHEFA\b/i, 'Washington Higher Education Facilities Authority')
    clean_title.gsub!(/\bWS\b/i, 'WorkSource')
    clean_title.gsub!(/\bWSF\b/i, 'Washington State Ferries')
    # clean_title.gsub!(/\bWSP\b/i, 'Washington State Penitentiary')
    clean_title.gsub!(/\bWSIPP\b/i, 'Washington State Institute for Public Policy')
    clean_title.gsub!(/\bWSP\b/i, 'Washington State Patrol')
    clean_title.gsub!(/\bWSR\b/i, 'Washington State Reformatory')
    clean_title.gsub!(/\bWTBBL\b/i, 'Washington Talking Book & Braille Library')
    clean_title.gsub!(/\bWTC\b/i, 'Writing & Tutoring Center')

    clean_title.gsub!(/\bCardiac\b/i, 'Cardiovascular')
    clean_title.gsub!(/\bCond\b/i, 'Conditioning')
    clean_title.gsub!(/\bGET\b/i, 'Guaranteed Education Tuition')
    clean_title.gsub!(/\bPAP\b/i, 'Professional Accounting Practice')
    clean_title.gsub!(/\bTECH\s&\sASSESS\b/i, 'Technology & Assessment')
    clean_title.gsub!(/\bSBOH\b/i, 'State Board of Health')
    clean_title.gsub!(/\bSo Sound\b/i, 'South Sound')

    clean_title.gsub!(/\bBg\b/i, 'Background')
    clean_title.gsub!(/\bRCL\b/i, 'Roads to Community Living')
    clean_title.gsub!(/\bSno\/King\b/i, 'Sno-King')

    clean_title.gsub!(/\bNW\b/i, 'Northwest')
    clean_title.gsub!(/\bSW\b/i, 'Southwest')
    clean_title.gsub!(/\bNC\b/i, 'North Central')
    clean_title.gsub!(/\bSc\b/i, 'Science') if clean_title.match?(/\bmath\b/i)
    clean_title.gsub!(/\bSC\b/i, 'South Central')

    clean_title = MiniLokiC::Formatize::Cleaner.job_titles_clean(clean_title) unless ['Assistant Superintendent SP,SE,SI, &FA', 'AD/EX Moonlight Appointment', "AD/Head Men's Basketball Coach", 'SSC Reg Tech Assistant Program Support'].include?(clean_title)

    puts clean_title.cyan
    clean_title.gsub!(/\b(?<=Director)(,\s|\s-\s|\s)((?=Fiscal)|(?=International)|(?=McIntyre)|(?=Organizational)|(?=Program)|(?=Public)|(?=Regulatory)|(?=Security))/i, ' of ')
    puts clean_title.red
    clean_title.gsub!(/\b(?<=President)(,\s|\s-\s|\s)(?=Administrative)/i, ' of ')

    clean_title.gsub!(/\bADSA\b/i, 'ADSA')
    clean_title.gsub!(/\bALT\b/i, 'ALT')
    clean_title.gsub!(/\bAMERI\b/i, 'AMERI')
    clean_title.gsub!(/\bAWV\b/i, 'AWV')
    clean_title.gsub!(/\bCCRSS\b/i, 'CCRSS')
    clean_title.gsub!(/\bEA\b/i, 'EA')
    clean_title.gsub!(/\bITMR(\d)?\b/i, 'ITMR\1')
    clean_title.gsub!(/\bLFO\/COS\/CCD\b/i, 'LFO/COS/CCD')
    clean_title.gsub!(/\bMcIntyre\b/i, 'McIntyre')
    clean_title.gsub!(/\bGU SOAR\b/i, 'GEAR UP SOAR')
    # clean_title.gsub!(/\bGU SOAR\b/i, 'GEAR UP Success, Opportunity, Affordability and Rigor, Relevance and Relationships')
    clean_title.gsub!(/\bHS21\b/i, 'HS21')
    clean_title.gsub!(/\b(GEAR\sUP|GU)\b/i, 'GEAR UP')
    clean_title.gsub!(/\bORCHD\b/i, 'ORCHD')
    clean_title.gsub!(/\bPLRP\b/i, 'PLRP')
    clean_title.gsub!(/\bRG(\d)\b/i, 'RG\1')
    clean_title.gsub!(/\bRAI\b/i, 'RAI')
    clean_title.gsub!(/\bSOAR\b/i, 'SOAR')
    clean_title.gsub!(/\bSSC\b/i, 'SSC')
    clean_title.gsub!(/\bSSCO(\d)?\b/i, 'SSCO\1')
    clean_title.gsub!(/\bSTEM\b/i, 'STEM')
    clean_title.gsub!(/\bWCR\b/i, 'WCR')
    clean_title.gsub!(/\bWES\b/i, 'WES')
    clean_title.gsub!(/\bWMS(\d)?\b/i, 'WMS \1')
    clean_title.gsub!(/\bWPR\b/i, 'WPR')
    clean_title.gsub!(/\bWSD\b/i, 'WSD')
    clean_title.gsub!(/\bWWCC\b/i, 'WWCC')
    clean_title.gsub!(/\bWYLIE\b/i, 'WYLIE')
    clean_title.squeeze!(' ')

    puts clean_title
    puts "#{item[:raw_column]} >>> #{clean_title}".cyan if item[:raw_column] != clean_title
    insert_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(clean_title)}'
      WHERE id = #{item['id']}
        AND #{table_info[:raw_column]}='#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL

    puts insert_query
    route.query(insert_query)
  end
end

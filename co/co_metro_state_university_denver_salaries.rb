# Creator:      Sergii Butrymenko
# Dataset Name: Colorado Metropolitan State University of Denver Salaries FOIA
# Task #:       116
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/761
# Dataset Link: https://lokic.locallabs.com/data_sets/574
# Created:      May 2023

# ruby mlc.rb --tool="clean::co::co_metro_state_university_denver_salaries"
# ruby mlc.rb --tool="clean::co::co_metro_state_university_denver_salaries" --mode='department'
# ruby mlc.rb --tool="clean::co::co_metro_state_university_denver_salaries" --mode='name'
# ruby mlc.rb --tool="clean::co::co_metro_state_university_denver_salaries" --mode='title'

def execute(options = {})
  route = C::Mysql.on(DB01, 'foia_salaries_gather')
  table_description = {
    department: {
      raw_table: 'raw_hi_ed__co_metro_state_university_denver_salaries',
      clean_table: 'raw_hi_ed__co_metro_state_university_denver_salaries_dpts_clean',
      raw_column: 'department',
      clean_column: 'department_clean',
    },
    name: {
      raw_table: 'raw_hi_ed__co_metro_state_university_denver_salaries',
      clean_table: 'raw_hi_ed__co_metro_state_university_denver_salaries_names_clean',
      raw_column: 'name',
      clean_column: 'name_clean',
    },
    title: {
      raw_table: 'raw_hi_ed__co_metro_state_university_denver_salaries',
      clean_table: 'raw_hi_ed__co_metro_state_university_denver_salaries_ttls_clean',
      raw_column: 'title',
      clean_column: 'title_clean',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :department
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    check_clean_departments(table_info, route)
  when :name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_names(table_info, route)
  when :title
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
    text: "*[CLEANING #116] DS #574 Colorado Metropolitan State University of Denver Salaries FOIA* \n>#{type} #{message}",
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
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
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

def check_clean_departments(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  departments_list = route.query(query).to_a
  return if departments_list.empty?
  message_to_slack("#{departments_list.count} new departments were added into *db01.foia_salaries_gather.raw_hi_ed__co_metro_state_university_denver_salaries_dpts_clean* and should be cleaned by editors", :warning)
end

def clean_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  return if names_list.empty?

  names_list.each do |row|
    clean_name = row
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    result_name = row[table_info[:raw_column]].dup
    result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name)
    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of
    clean_name[table_info[:clean_column]] = result_name
    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    puts update_query
    route.query(update_query)
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

    clean_title.gsub!(/\bAccessibilityTechn Manager\b/i, 'Accessibility Technical Manager')
    clean_title.gsub!(/\bAdmin Asst\b/i, 'Administrative Assistant')
    clean_title.gsub!(/\bAdmin Assistant II\b/i, 'Administrative Assistant 2')
    clean_title.gsub!(/\bEarly Child Education Specialist\b/i, 'Early Childhood Education Specialist')
    clean_title.gsub!(/\bStaff Psychologist & Assoc Dir OT\b/i, 'Staff Psychologist & Associate Director OT')
    clean_title.gsub!(/\bAdmin Asst to the Assoc to the Pres for Diversity\b/i, 'Administrative Assistant to the Associate to the President for Diversity')
    clean_title.gsub!(/\bAssist Dir of Corp & Found Rel\b/i, 'Assistant Director of Corporate & Foundation Relations')
    clean_title.gsub!(/\bAsst Dir of Corp and Foundation Relations\b/i, 'Assistant Director of Corporate & Foundation Relations')
    clean_title.gsub!(/\bAsst ot the Assoc VP for Curriculum and Acad Effectiveness\b/i, 'Assistant to the Associate Vice President for Curriculum and Academic Effectiveness')
    clean_title.gsub!(/\bAssoc Dir, Admissions\/Transfer Services\b/i, 'Associate Director, Admissions & Transfer Services')
    clean_title.gsub!(/\bAssoc Dir\. Admissions-Transf Srvs\b/i, 'Associate Director, Admissions & Transfer Services')
    clean_title.gsub!(/\bApplication Services Business Analyst\b/i, 'Applications Services Business Analyst')
    clean_title.gsub!(/\bApplications Services, Business Analyst\b/i, 'Applications Services Business Analyst')
    clean_title.gsub!(/\bAssoc Director of Development for Fnd Relations\b/i, 'Associate Director of Development for Foundation Relations')
    clean_title.gsub!(/\bCompliance & Prev Coordinator\b/i, 'Compliance & Prevention Coordinator')
    clean_title.gsub!(/\bData Manager, College of Prof Studies\b/i, 'Data Manager, College of Professional Studies')
    clean_title.gsub!(/\bCollege of Prof Studies\b/i, 'College of Professional Studies')
    clean_title.gsub!(/\bDir Master of Health Admin\b/i, 'Director Master of Health Administration')
    clean_title.gsub!(/\bDirector of Office of Social Work Student Services, Administraion & Finance\b/i, 'Director of Office of Social Work Student Services, Administration & Finance')
    clean_title.gsub!(/\bDir of Office of Social Work Student Services, Admin & Finance\b/i, 'Director of Office of Social Work Student Services, Administration & Finance')
    clean_title.gsub!(/\bEnviron & Emergency Prep Mngr\b/i, 'Environmental & Emergency Preparedness Manager ')
    clean_title.gsub!(/\bExecutive Asst to the VP & to Assoc VP - Admin Fin & Facilities\b/i, 'Executive Assistant to the Vice President & to Associate Vice President Administration Finance & Facilities')
    clean_title.gsub!(/\bInterim Dir of Clinical Exper & Partnerships\b/i, 'Interim Director of Clinical Experiences & Partnerships')
    clean_title.gsub!(/\bHuman Subjects Protection Program Manager\b/i, 'Human Subject Protection Program Manager')
    clean_title.gsub!(/\bLockeed Martin Endowed, Director of Advanced Manufacturing Sciences \(AMSI\) Institute\b/i, 'Lockheed Martin Endowed, Director of Advanced Manufacturing Sciences Institute (AMSI)')

    clean_title.gsub!(/\bAero Tech Specialist\b/i, 'Aero Technical Specialist')
    clean_title.gsub!(/\bAdministrative Asst III Writing Program - Freshman Comp \(ENG\)\b/i, 'Administrative Assistant 3 Writing Program - Freshman Composition (English)')
    clean_title.gsub!(/\bAdv Comm & Giv\b/i, 'Advancement Communications & Giving')
    clean_title.gsub!(/\bAdvisor & Admin Coord inator\b/i, 'Advisor & Administrative Coordinator')
    clean_title.gsub!(/\bAsst. Director of Academic Achieve\b/i, 'Assistant Director of Academic Achievement')
    clean_title.gsub!(/\bAssoc Dir. Admissions-Transf Srvs\b/i, 'Associate Director Admissions-Transfer Services')
    clean_title.gsub!(/\bAdmin\/Finl Services Spec\b/i, 'Administrative & Financial Services Specialist')
    clean_title.gsub!(/\bAssoc Dir CMEI Sudent Orgs & Ldr\b/i, 'Associate Director of the Center for Multicultural Engagement and Inclusion (CMEI) for Student Organizations & Leadership')
    clean_title.gsub!(/\bAssoc VP for Admin, Finance, & Facilities and Chief Financial Officer\b/i, 'Associate Vice President for Administration, Finance, & Facilities and Chief Financial Officer')
    clean_title.gsub!(/\bAVP for Cirriculum & Policy Development\b/i, 'Associate Vice President for Curriculum & Policy Development')
    clean_title.gsub!(/\bCCWSC Admin\/Fin Serv Sped\b/i, 'CCWSC Administrative & Financial Services Special Education')
    clean_title.gsub!(/\bDirector, CAMP\b/i, 'Director, CAMP')
    clean_title.gsub!(/\bDirector, OSRP\b/i, 'Director, Office of Sponsored Research and Programs (OSRP)')
    clean_title.gsub!(/\bDirector, Student Health Clinic\b/i, 'Director, Student Health Clinic')
    clean_title.gsub!(/\bEarly Literacy and PAT Parent Educator\b/i, 'Early Literacy and Parents as Teachers Parent Educator')
    clean_title.gsub!(/\bHIPPY\/PAT Coord\b/i, 'Home Instruction for Parents of Preschool Youngsters/Parents as Teachers Coordinator')
    clean_title.gsub!(/\bInterim Grants Admin Coord\b/i, 'Interim Grants Administration Coordinator')
    clean_title.gsub!(/\bPAT Asst & Liaison to Spanish Speaking Families\b/i, 'Parents as Teachers (PAT) Assistant & Liaison to Spanish Speaking Families')
    clean_title.gsub!(/\bPAT Parent Educ Specialist\b/i, 'Parents as Teachers (PAT) Parent Education Specialist')
    clean_title.gsub!(/\bDirector,Center for Urban Education\b/i, 'Director, Сenter for Urban Education')

    clean_title.gsub!(/\.(?!(\s|,|$))/, '. ')
    clean_title.sub!(/,?\sand\sa$/i, '')

    # clean_title = clean_title.gsub(/([[:lower:]\d])([[:upper:]])/, '\1 \2').gsub(/([^-\d])(\d[-\d]*( |$))/,'\1 \2').gsub(/([[:upper:]])([[:upper:]][[:lower:]\d])/, '\1 \2').gsub(/(?<!\s)&/, ' &').gsub(/&(?!\s)/, '& ')
    clean_title.gsub!(/\bAdmin(istraion)? & Finance\b/i, 'Administration & Finance')
    clean_title.gsub!(/\bAdmin\/Finl?\b/i, 'Administrative & Financial')
    clean_title.gsub!(/\bAdmin\sAss(istant|t)\b/i, 'Administrative Assistant')
    clean_title.gsub!(/\bAdmin, Finance, & Facilities\b/i, 'Administration, Finance, & Facilities')
    clean_title.gsub!(/\bAero Tech\b/i, 'Aero Technical')
    clean_title.gsub!(/\bFreshman Comp\b/i, 'Freshman Composition')
    clean_title.gsub!(/\bOrgs & Ldr\b/i, 'Organizations & Leadership')
    clean_title.gsub!(/\bReg Trsfr Eval & PL \b/i, 'Registrar Transfer Evaluation & Prior Learning ')

    clean_title.gsub!(/\bAA\b/i, 'Academic Affairs')
    clean_title.gsub!(/\bAchieve\b/i, 'Achievement')
    clean_title.gsub!(/\bAccessibilityTechn\b/i, 'Accessibility Technical')
    clean_title.gsub!(/\bAdvixor\b/i, 'Advisor')
    clean_title.gsub!(/\bAMSI\b/i, 'Advanced Manufacturing Sciences Institute') unless clean_title.match?(/\bAdvanced\sManufacturing\sSciences\b/i)
    clean_title.gsub!(/\bAES\b/i, 'Aerospace Science')
    clean_title.gsub!(/\bAna\b/i, 'Analyst')
    clean_title.gsub!(/\bCAVEA\b/i, 'Center for Advanced Visualization & Experiential Analysis (CAVEA)')
    clean_title.gsub!(/\bCMEI\b/i, 'Center for Multicultural Engagement and Inclusion (CMEI)') unless clean_title.match?(/\bMulticultural Engagement\b/)
    clean_title.gsub!(/\bCVA\b/i, 'Center for Visual Art (CVA)')
    clean_title.gsub!(/\bCCMLS\b/i, 'Colorado Center for Medical Laboratory Science (CCMLS)')
    clean_title.gsub!(/\bCRM\b/i, 'Constituent Relationship Management')
    clean_title.gsub!(/\bCorp\b/i, 'Corporate')
    clean_title.gsub!(/\bCrss\b/i, 'Cross')
    clean_title.gsub!(/\bDept\b/i, 'Department')
    clean_title.gsub!(/\bENG\b/i, 'English')
    clean_title.gsub!(/\bEM\b/i, 'Enrollment Management')
    clean_title.gsub!(/\bExper\b/i, 'Experiences')
    clean_title.gsub!(/\bFA\b/i, 'Financial Aid')
    clean_title.gsub!(/\bFLP\b/i, 'Family Literacy Program')
    clean_title.gsub!(/\b(Fnd|Found)\b/i, 'Foundation')
    clean_title.gsub!(/\bFunds\b/i, 'Foundations')
    clean_title.gsub!(/\bGiv\b/i, 'Giving')
    clean_title.gsub!(/\bHRSA\b/i, 'Health Resources and Services Administration')
    clean_title.gsub!(/\bHIPPY\b/i, 'Home Instruction for Parents of Preschool Youngsters')
    clean_title.gsub!(/\bInstit\b/i, 'Institute')
    clean_title.gsub!(/\bInt\.?\b/i, 'International')
    clean_title.gsub!(/\bIntv\b/i, 'Interviewing')
    clean_title.gsub!(/\bLAS\b/i, 'Liberal Arts and Sciences')
    clean_title.gsub!(/\bLockeed\sMartin\b/i, 'Lockheed Martin')
    clean_title.gsub!(/\bMSU\b/i, 'Metropolitan State University')
    clean_title.gsub!(/\bOrgs\b/i, 'Organizations')
    clean_title.gsub!(/\bOSRP\b/i, 'Office of Sponsored Research and Programs (OSRP)') unless clean_title.match?(/\bSponsored Research\b/i)
    clean_title.gsub!(/\bOWOW\b/i, 'One World One Water (OWOW)') unless clean_title.match?(/\bOne World One Water\b/i)
    clean_title.gsub!(/\bOWOWC\b/i, 'One World One Water Center (OWOWC)') unless clean_title.match?(/\bOne World One Water\b/i)
    clean_title.gsub!(/\bPAT\b/i, 'Parents as Teachers (PAT)') unless clean_title.match?(/\bParents as Teachers\b/i)
    clean_title.gsub!(/\bPL\b/i, 'Prior Learning')
    clean_title.gsub!(/\bPrep\b/i, 'Preparedness')
    clean_title.gsub!(/\bPrev\b/i, 'Prevention')
    clean_title.gsub!(/\bProced\b/i, 'Procedures')
    clean_title.gsub!(/\bPS\b/i, 'Professional Studies')
    clean_title.gsub!(/\bRecon\b/i, 'Reconciliation')
    clean_title.gsub!(/\bRecruit\b/i, 'Recruitment ')
    clean_title.gsub!(/\bResol\b/i, 'Resolution')
    clean_title.gsub!(/\bRetn?\b/i, 'Retention')
    clean_title.gsub!(/\bSchol\b/i, 'Scholarship')
    clean_title.gsub!(/\bSOE\b/i, 'School of Education')
    clean_title.gsub!(/\b(Spec|Splst)\b/i, 'Specialist')
    clean_title.gsub!(/\bSped\b/i, 'Special Education')
    clean_title.gsub!(/\btechn\b/i, 'Technical')
    clean_title.gsub!(/\bOT(?=\s)/i, 'to')
    clean_title.gsub!(/\b(Transf|Trsfr)\b/i, 'Transfer')
    clean_title.gsub!(/\bUA\b/i, 'University Advancement')

    unless ['Staff Psychologist & Associate Director OT'].include?(clean_title)
      clean_title = MiniLokiC::Formatize::Cleaner.job_titles_clean(clean_title)
    end

    clean_title.gsub!(/\bAMSI\b/i, 'AMSI')
    clean_title.gsub!(/\bAR\b/i, 'AR')
    clean_title.gsub!(/\bCAVEA\b/i, 'CAVEA')
    clean_title.gsub!(/\bCAMP\b/i, 'CAMP')
    clean_title.gsub!(/\bCBUS\b/i, 'CBUS')
    clean_title.gsub!(/\bCOB\b/i, 'COB')
    clean_title.gsub!(/\bLGLBT\b/i, 'LGLBT')
    clean_title.gsub!(/\bAR\b/i, 'AR')
    clean_title.gsub!(/\bHIS\b/i, 'HIS')
    clean_title.gsub!(/\bHTE\b/i, 'HTE')
    clean_title.gsub!(/\bAP\b/i, 'AP')
    clean_title.gsub!(/\bMET\b/i, 'MET')
    clean_title.gsub!(/\bUGS\b/i, 'UGS')
    clean_title.gsub!(/\bCLAS\b/i, 'CLAS')
    clean_title.gsub!(/\bCMEI\b/i, 'CMEI')
    clean_title.gsub!(/\bTRIO\b/i, 'TRIO')
    clean_title.gsub!(/\bALP\b/i, 'ALP')
    clean_title.gsub!(/\bCCMLS\b/i, 'CCMLS')
    clean_title.gsub!(/\bCFO\b/i, 'CFO')
    clean_title.gsub!(/\bCPS\b/i, 'CPS')
    clean_title.gsub!(/\bCVA\b/i, 'CVA')
    clean_title.gsub!(/\bSWA\b/i, 'SWA')
    clean_title.gsub!(/\bMHA\b/i, 'MHA')
    clean_title.gsub!(/\bСOB\b/i, 'COB')
    clean_title.gsub!(/\bOSRP\b/i, 'OSRP')
    clean_title.gsub!(/\bPAT\b/i, 'PAT')
    clean_title.gsub!(/\bSOAN\b/i, 'SOAN')
    clean_title.gsub!(/\bLGBTQ\b/i, 'LGBTQ')
    clean_title.gsub!(/\bCCWSC\b/i, 'CCWSC')
    clean_title.gsub!(/\bCHAMP\b/i, 'CHAMP')
    clean_title.gsub!(/\bM\s?&\s?W\b/i, 'M&W')
    clean_title.gsub!(/\bMH\s?&\s?HE\b/i, 'MH & HE')
    clean_title.gsub!(/\bITAM\b/i, 'ITAM')
    clean_title.gsub!(/\bSAP\b/i, 'SAP')
    clean_title.gsub!(/\bOWOW\b/i, 'OWOW')
    clean_title.gsub!(/\bOWOWC\b/i, 'OWOWC')
    clean_title.gsub!(/\bCOO\b/i, 'COO')
    clean_title.gsub!(/\bUX\b/i, 'UX')


    puts clean_title
    puts "#{item[:raw_column]} >>> #{clean_title}".cyan if item[:raw_column] != clean_title
    insert_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(clean_title)}'
      WHERE id = #{item['id']}
        AND #{table_info[:raw_column]}='#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL

    # puts insert_query
    route.query(insert_query)
  end
end

# Creator:      Kiril Kuzin
# Dataset Name: Arkansas public Employee Salaries
# Task #:       
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/764
# Dataset Link: https://lokic.locallabs.com/data_sets/464
# Created:      June 2023
#

# ruby mlc.rb --tool="clean::ar::ar_employee_salaries"
# ruby mlc.rb --tool="clean::ar::ar_employee_salaries" --mode='name'
# ruby mlc.rb --tool="clean::ar::ar_employee_salaries" --mode='title'
# ruby mlc.rb --tool="clean::ar::ar_employee_salaries" --mode='agency'

def execute(options = {})
  # begin
    route = C::Mysql.on(DB01, 'state_salaries__raw')
    table_description = {
      name: {
        raw_table: 'ar_employee_salaries',
        clean_table: 'ar_employee_salaries__names_clean',
        raw_column: 'employee_name',
        clean_column: 'employee_name_clean',
      },
      title: {
        raw_table: 'ar_employee_salaries',
        clean_table: 'ar_employee_salaries__position_titles_clean',
        raw_column: 'position_title',
        clean_column: 'position_title_clean',
      },
      agency: {
        raw_table: 'ar_employee_salaries',
        clean_table: 'ar_employee_salaries__agencies_clean',
        raw_column: 'agency',
        clean_column: 'agency_clean',
      }
    }
    where_part = options['where']
    mode = options['mode']&.to_sym
    table_info = table_description[mode]
    case mode
    when :name
      recent_date = get_recent_date(table_info, route)
      fill_table(table_info, recent_date, where_part, route)
      clean_names(table_info, route)
    when :title
      recent_date = get_recent_date(table_info, route)
      fill_table(table_info, recent_date, where_part, route)
      clean_titles(table_info, route)
    when :agency
      recent_date = get_recent_date(table_info, route)
      fill_table(table_info, recent_date, where_part, route)
      clean_agencies(table_info, route)
    else
      puts 'EMPTY'.black.on_yellow
    end
    route.close
  # rescue StandardError => e
  #   message = "Error: #{e.message}\nBacktrace: #{e.backtrace.join("\n")}"
  #   puts message
  #   #message_to_slack(message, type = :alert)
  # ensure
  #   route.close if route
  # end
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
    channel: 'U03FSA3LFSB',
    text: "*[CLEANING] #764 Arkansas public Employee Salaries* \n>#{type} #{message}",
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
    if table_info[:pattern_column]
      pattern = "#{table_info[:pattern_column]} VARCHAR(5) DEFAULT NULL,"
      name_parts = "first_name VARCHAR(255) DEFAULT NULL,\nmiddle_name VARCHAR(255) DEFAULT NULL,\nlast_name VARCHAR(255) DEFAULT NULL,\nsuffix VARCHAR(255) DEFAULT NULL,\n"
    elsif table_info[:clean_table].match(/__names_clean/)
      pattern = nil
      name_parts = "first_name VARCHAR(255) DEFAULT NULL,\nmiddle_name VARCHAR(255) DEFAULT NULL,\nlast_name VARCHAR(255) DEFAULT NULL,\nsuffix VARCHAR(255) DEFAULT NULL,\n"
    else
      pattern = nil
      name_parts = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{pattern}
         #{table_info[:clean_column]} VARCHAR(255),
         #{name_parts}
         #{type}
         #{state}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         cleaning_dev_name VARCHAR(15) DEFAULT 'Kiril Kuzin',
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

def clean_agencies(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  agencies_list = route.query(query).to_a
  return if agencies_list.empty?

  agencies_list.each do |row|
    clean_name = row
    # puts "#{clean_name[table_info[:raw_column]]}".cyan
    clean_agency = clean_name[table_info[:raw_column]].dup
  
    clean_agency = MiniLokiC::Formatize::Cleaner.org_clean(clean_agency)

    clean_agency.gsub!(/\bADWS\b/i, 'Arkansas Division of Workforce Services')
    clean_agency.gsub!(/\bDAH\b/i, 'Department of Arkansas Heritage')
    clean_agency.gsub!(/\bCERT\b/i, 'Certification') if clean_agency.match(/\bBoard\b/i)
    clean_agency.gsub!(/\bLIC\b/i, 'Licensing') if clean_agency.match(/\bBoard\b/i)
    clean_agency.gsub!(/\bAR\b/i, 'Arkansas') 
    clean_agency.gsub!(/\bPATH & AUD\b/i,'Pathology and Audiology')
    clean_agency.gsub!(/\bLieutenant Gov\b/i,'Lieutenant Governor')
    clean_agency.gsub!(/\bENG\b/i, 'Engineers')
    clean_agency.gsub!(/\bSURV\b/i, 'Surveyors')
    clean_agency.gsub!(/\bEXAM\b/i, 'Examiners')
    clean_agency.gsub!(/\bINT DSN\b/i, 'Interior Design')
    clean_agency.gsub!(/\bLand\b/i, 'Landscape')
    clean_agency.gsub!(/\bOFC\b/i, 'Office')
    clean_agency.gsub!(/\bDFA\b/i, 'Department of Finance and Administration')
    clean_agency.gsub!(/\bENF\b/i, 'Enforcement')
    clean_agency.gsub!(/\bLang\b/i, 'Language')
    clean_agency.gsub!(/\bTSS\b/i, 'Transformation and Shared Services')
    clean_agency.gsub!(/\bInfo Syst\b/i, 'Information Systems')
    clean_agency.gsub!(/\bReg\b/i, 'Registration') if clean_agency.match(/\bBoard\b/i)
    clean_agency.gsub!(/\bAlcoh Bev\b/i, 'Alcoholic Beverage')
    clean_agency.gsub!(/\bDiv\b/i, 'Division')
    clean_agency.gsub!(/\bJud\b/i, 'Judicial')
    clean_agency.gsub!(/\bTour\b/i, 'Tourism')
    clean_agency.gsub!(/\bAdmin(istratn)?\b/i, 'Administration')
    clean_agency.gsub!(/\bDisc\b/i, 'Discipline')
    clean_agency.gsub!(/\bMed\b/i, 'Medical')
    clean_agency.gsub!(/\bGen\b/i, 'General')

    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of

    clean_name[table_info[:clean_column]] = clean_agency
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query
    route.query(update_query)
  end
end

def init_name_splitter
  require_relative '../../namesplit/namesplit'
  ns_options = {
    'host' => DB01,
    'database' => 'state_salaries__raw',
    'table' => 'ar_employee_salaries__names_clean',
    'no-business' => 'enabled',
    'no_double_lastname' => 'enabled',
    'mode' => 'split',
    'field' => 'employee_name',
    'no_print_results' => 'enabled',
    'style' => 'fml'
  }
  NameSplit.new(ns_options)
end

def clean_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  return if names_list.empty?

  splitter = init_name_splitter
  names_list.each do |row|
    clean_name = row
    splitted_name = nil

    result_name = row[table_info[:raw_column]].dup
   # result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name, false) 
    result_name.gsub!(/^(cpl\.?|tfc|tpr\.?)\s+(.+)/i, '\2 \1')
    splitted_name, _action = splitter.split_record('full_name' => result_name)
    clean_name[table_info[:clean_column]] = splitted_name[:full_name]

    puts "#{row[table_info[:raw_column]]} => #{splitted_name[:full_name]}".cyan

    update_query = <<~SQL
    UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(splitted_name[:full_name])}',
          first_name = '#{escape(splitted_name[:first_name])}',
          middle_name = '#{escape(splitted_name[:middle_name])}',
          last_name = '#{escape(splitted_name[:last_name])}',
          suffix = '#{escape(splitted_name[:suffix])}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
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
    clean_title = item[table_info[:raw_column]].dup.gsub(/\.(?!(\s|,|$))/, '. ').sub(/,?\sand\sa$/i, '')

    # clean_title = clean_title.gsub(/([[:lower:]\d])([[:upper:]])/, '\1 \2').gsub(/([^-\d])(\d[-\d]*( |$))/,'\1 \2').gsub(/([[:upper:]])([[:upper:]][[:lower:]\d])/, '\1 \2').gsub(/(?<!\s)&/, ' &').gsub(/&(?!\s)/, '& ')

    clean_title.gsub!(/\bASP\b/i, 'Arkansas State Police')
    clean_title.gsub!(/\bAGFC\b/i, 'Arkansas Game and Fish Commission')
    clean_title.gsub!(/\bADPHT\b/i, 'Arkansas Department of Parks, Heritage and Tourism')
    clean_title.gsub!(/\bADH\b/i, 'Arkansas Department of Health')
    clean_title.gsub!(/\bAR\b/i, 'Arkansas')
    clean_title.gsub!(/\bCA\b/i, 'Court of Appeals')
    
    clean_title = MiniLokiC::Formatize::Cleaner.job_titles_clean(clean_title)

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

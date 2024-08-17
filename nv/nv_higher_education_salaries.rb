# Creator:      Sergii Butrymenko
# Dataset Name: Nevada Higher Education Salaries
# Task #:       116
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/616
# Dataset Link: https://lokic.locallabs.com/data_sets/484
# Created:      May 2023
#
# NOTE:         Name pattern (FML-LFM) varies by year

# ruby mlc.rb --tool="clean::nv::nv_higher_education_salaries"
# ruby mlc.rb --tool="clean::nv::nv_higher_education_salaries" --mode='department'
# ruby mlc.rb --tool="clean::nv::nv_higher_education_salaries" --mode='name'
# ruby mlc.rb --tool="clean::nv::nv_higher_education_salaries" --mode='title'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    department: {
      raw_table: 'nv_higher_education_salaries',
      clean_table: 'nv_higher_education_salaries__department_clean',
      raw_column: 'department',
      clean_column: 'department_clean',
    },
    name: {
      raw_table: 'nv_higher_education_salaries',
      clean_table: 'nv_higher_education_salaries__names_clean',
      raw_column: 'name',
      pattern_column: 'name_pattern',
      clean_column: 'name_clean',
    },
    title: {
      raw_table: 'nv_higher_education_salaries',
      clean_table: 'nv_higher_education_salaries__job_title_clean',
      raw_column: 'job_title',
      clean_column: 'job_title_clean',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :department
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_departments(table_info, route)
  when :name
    recent_date = get_recent_date(table_info, route)
    fill_name_table(table_info, recent_date, where_part, route)
    clean_names_with_pattern(table_info, route)
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
    text: "*[CLEANING #116] ST #616 Nevada Higher Education Salaries* \n>#{type} #{message}",
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

def fill_name_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]},
           IF(year BETWEEN 2010 AND 2013, 'lfm', 'fml') AS #{table_info[:pattern_column]},
           MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.id IS NULL
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
      INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, #{table_info[:pattern_column]}, scrape_date)
      VALUES
    SQL
    part.each do |item|
      insert_query << "('#{escape(item[table_info[:raw_column]])}', '#{item[table_info[:pattern_column]]}', '#{item['scrape_date']}'),"
    end
    insert_query = "#{insert_query.chop};"
    # puts insert_query.red
    route.query(insert_query)
  end
end

def clean_departments(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  depatments_list = route.query(query).to_a
  return if depatments_list.empty?

  depatments_list.each do |row|
    clean_name = row
    # puts "#{clean_name[table_info[:raw_column]]}".cyan
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(clean_name[table_info[:raw_column]].dup)
    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of
    clean_name[table_info[:clean_column]] = result_name
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

def clean_names_with_pattern(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}, #{table_info[:pattern_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  return if names_list.empty?

  ns_fml, ns_lfm = init_name_splitter
  names_list.each do |row|
    clean_name = row
    splitted_name = nil

    # puts "#{clean_name[table_info[:raw_column]]}".cyan
    p row
    result_name = row[table_info[:raw_column]].dup
    if row[table_info[:pattern_column]] == 'fml'
      result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name, false)
      splitted_name, _action = ns_fml.split_record('full_name' => row[table_info[:raw_column]])
    else
      result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name)
      splitted_name, _action = ns_lfm.split_record('full_name' => row[table_info[:raw_column]])
    end

    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of
    clean_name[table_info[:clean_column]] = result_name
    # puts JSON.pretty_generate(clean_name).yellow
    # puts JSON.pretty_generate(splitted_name).green
    last_name = splitted_name[:last_name]
    last_name += " #{splitted_name[:last_name2]}" unless splitted_name[:last_name2].empty?
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(splitted_name[:full_name])}',
          first_name = '#{escape(splitted_name[:first_name])}',
          middle_name = '#{escape(splitted_name[:middle_name])}',
          last_name = '#{escape(last_name)}',
          suffix = '#{escape(splitted_name[:suffix])}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    puts update_query
    route.query(update_query)
  end
end

def init_name_splitter
  require_relative '../../namesplit/namesplit'
  ns_options = {
    'host' => DB01,
    'database' => 'usa_raw',
    'table' => 'nv_higher_education_salaries__names_clean',
    'no-business' => 'enabled',
    'no-double_lastname' => 'enabled',
    'mode' => 'split',
    'field' => 'name',
    'no_print_results' => 'enabled'
  }

  ns_fml = NameSplit.new(ns_options.merge({'style' => 'fml'}))
  ns_lfm = NameSplit.new(ns_options.merge({'style' => 'lfm'}))
  [ns_fml, ns_lfm]
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

    # clean_title.gsub!(/\bAdv\b\.?/i, 'Advancement') if clean_title.match?(/Alum(ni)?\sAff(airs)?/i)
    # clean_title.gsub!(/\bAdv\b\.?/i, 'Advanced')  if clean_title.match?(/\bnurse\b/i)
    # clean_title.gsub!(/\bAdv\b\.?/i, 'Advisor')

    # clean_title.gsub!(/\b(CRD|CDR)\b/i, 'Coordinator')
    # clean_title.gsub!(/\bC-MAN\b/i, 'Craftsman')
    # clean_title.gsub!(/\bE&CC\b/i, 'Emergency & Critical Care')
    # clean_title.gsub!(/\bCC\b/i, 'Critical Care')
    # clean_title.gsub!(/\bDis\b/i, 'Disorder')
    # clean_title.gsub!(/\bEr\b/i, 'Emergency')
    # clean_title.gsub!(/\bEO\b/i, 'Equal Opportunity')
    # clean_title.gsub!(/\bFB\b/i, 'Football')
    # clean_title.gsub!(/\bHPWT\b/i, 'High Performance Work Team')
    # clean_title.gsub!(/\bHous\b/i, 'Housing')
    # clean_title.gsub!(/\bInit\/Adv\b/i, 'Initiatives/Advancement')
    # clean_title.gsub!(/\bInit\b/i, 'Initiatives')
    # clean_title.gsub!(/\bLMS\b/i, 'Learning Management System')
    # clean_title.gsub!(/\bLMHC\b/i, 'Licensed Mental Health Counselor')
    # clean_title.gsub!(/\bMech\b/i, 'Mechanic')
    # clean_title.gsub!(/\bOpe\b/i, 'Operations')
    # clean_title.gsub!(/\bPtnr\b/i, 'Partner')
    # clean_title.gsub!(/\bPopulation\sA\b/i, 'Population Assessment')
    # clean_title.gsub!(/\bReceiv\b/i, 'Receivable')
    # clean_title.gsub!(/\bRes\b/i, 'Residence')
    # clean_title.gsub!(/\bSBDC\b/i, 'Small Business Development Center')
    # clean_title.gsub!(/\bSpect\b/i, 'Spectrum')
    # clean_title.gsub!(/\bSpons\b/i, 'Sponsored')
    # clean_title.gsub!(/\bT(e?chn)?\b/i, 'Technician')
    # clean_title.gsub!(/\bTechn(ol|st)\b/i, 'Technologist')
    # clean_title.gsub!(/\bTelecom\b/i, 'Telecommunications')
    # clean_title.gsub!(/\bVet\b/i, 'Veterinary')

    clean_title = MiniLokiC::Formatize::Cleaner.job_titles_clean(clean_title)

    # clean_title.gsub!(/\bLLL\b/i, '3')
    # clean_title.gsub!(/\bERP\b/i, 'ERP')
    # clean_title.gsub!(/\bFLMNH\b/i, 'FLMNH')
    # clean_title.gsub!(/\bOne\s?Stop\b/i, 'OneStop')
    # clean_title.gsub!(/\bUFHealth\b/i, 'UF Health')
    # clean_title.gsub!(/\bHealth Care Administrator\b/i, 'Health Care Administration')
    # clean_title.gsub!(/\bResearch Administrator\b/i, 'Research Administration')
    # clean_title.gsub!(/\bUniversitycontroller\b/i, 'University Controller')
    # clean_title.gsub!(/\bResidence\/Fellowship\b/i, 'Residency/Fellowship')
    # clean_title.gsub!(/\bAlumni\sA\b/i, 'Alumni Affairs')


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

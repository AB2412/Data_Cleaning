# Creator:      Sergii Butrymenko
# Dataset Name: Nevada Public Employee Salary
# Task #:       126
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/436
# Dataset Link: https://lokic.locallabs.com/data_sets/69
# Created:      June 2023

# ruby mlc.rb --tool="clean::nv::nv_public_employee_salary"
# ruby mlc.rb --tool="clean::nv::nv_public_employee_salary" --mode='department'
# ruby mlc.rb --tool="clean::nv::nv_public_employee_salary" --mode='name'
# ruby mlc.rb --tool="clean::nv::nv_public_employee_salary" --mode='title'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    department: {
      raw_table: 'nv_public_employee_salary',
      clean_table: 'nv_public_employee_salary_uniq_agencies',
      raw_column: 'employer',
      clean_column: 'employer_clean',
    },
    name: {
      raw_table: 'nv_public_employee_salary',
      clean_table: 'nv_public_employee_salary__names_clean',
      raw_column: 'full_name',
      # pattern_column: 'name_pattern',
      clean_column: 'full_name_clean',
    },
    title: {
      raw_table: 'nv_public_employee_salary',
      clean_table: 'nv_public_employee_salary__job_titles_clean',
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
    text: "*[CLEANING #126] ST #436 Nevada Public Employee Salary* \n>#{type} #{message}",
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
    if table_info[:raw_column] == 'full_name'
      name_parts = "first_name VARCHAR(255) DEFAULT NULL,\nmiddle_name VARCHAR(255) DEFAULT NULL,\nlast_name VARCHAR(255) DEFAULT NULL,\nsuffix VARCHAR(255) DEFAULT NULL,\n"
    else
      name_parts = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{name_parts}
         #{type}
         #{state}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         cleaning_dev_name VARCHAR(255) NOT NULL,
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
      INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date, cleaning_dev_name)
      VALUES
    SQL
    part.each do |item|
      insert_query << "('#{escape(item[table_info[:raw_column]])}','#{item['scrape_date']}','Sergii Butrymenko'),"
    end
    insert_query = "#{insert_query.chop};"
    puts insert_query.red
    route.query(insert_query)
  end
end

def fill_name_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]},
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
      INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date, cleaning_dev_name)
      VALUES
    SQL
    part.each do |item|
      insert_query << "('#{escape(item[table_info[:raw_column]])}', '#{item['scrape_date']}','Sergii Butrymenko'),"
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

def clean_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  return if names_list.empty?

  ns_fml = init_name_splitter(table_info)
  names_list.each do |row|
    clean_name = row
    splitted_name = {}

    p row
    result_name = row[table_info[:raw_column]].dup
    result_name.sub!(/^\(Do Not Use\)\s/i, '')
    unless result_name.empty?
      if result_name.include?(',')
        result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name)
      else
        result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name, false)
      end
      # begin
        splitted_name, _action = ns_fml.split_record('full_name' => result_name)
      # rescue
      #   result_name = 'error in split'
      # end

      # puts splitted_name
    end

    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of
    clean_name[table_info[:clean_column]] = result_name
    # puts JSON.pretty_generate(clean_name).yellow
    # puts JSON.pretty_generate(splitted_name).green
    last_name = splitted_name[:last_name]
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

def init_name_splitter(table_info)
  require_relative '../../namesplit/namesplit'
  ns_options = {
    'host' => DB01,
    'database' => 'usa_raw',
    'table' => table_info[:clean_table],
    'no-business' => 'enabled',
    'no-double_lastname' => 'enabled',
    'mode' => 'split',
    'style' => 'fml',
    'field' => table_info[:clean_column],
    'no_print_results' => 'enabled'
  }

  NameSplit.new(ns_options.merge({}))
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
    clean_title = item[table_info[:raw_column]].dup.sub(/^\./, '').gsub(/\.(?!(\s|,|$))/, '. ').sub(/,?\sand\sa$/i, '')
    clean_title = clean_title[1...-1] if clean_title[0] == '"' && clean_title[-1] == '"'

    # clean_title = clean_title.gsub(/([[:lower:]\d])([[:upper:]])/, '\1 \2').gsub(/([^-\d])(\d[-\d]*( |$))/,'\1 \2').gsub(/([[:upper:]])([[:upper:]][[:lower:]\d])/, '\1 \2').gsub(/(?<!\s)&/, ' &').gsub(/&(?!\s)/, '& ')

    # clean_title.gsub!(/\bInstructorPart-TimeCredit\b/i, 'Instructor Part-Time Credit')
    # clean_title.gsub!(/\bCc Professor,English\b/i, 'Community College Professor, English')
    #
    # clean_title.gsub!(/\bT(ea)?ch\/(credit|CRDT)\b/i, 'Teaching/Credit')
    # clean_title.gsub!(/\bTCH\/NON\sCRDT\b/i, 'Teaching/Non-Credit')

    clean_title = MiniLokiC::Formatize::Cleaner.job_titles_clean(clean_title)

    # clean_title.gsub!(/\bUNLV\b/i, 'University of Nevada-Las Vegas')
    # clean_title.gsub!(/\bPti\b/i, 'PTI')
    # clean_title.gsub!(/\bHVACR\b/i, 'HVACR')
    # clean_title.gsub!(/\bLOA\b/i, 'LOA')
    # clean_title.gsub!(/\bCSN\b/i, 'College of Southern Nevada')


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

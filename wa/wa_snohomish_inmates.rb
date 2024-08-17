# Creator:      Aqeel Anwar
# Dataset Name: Snohomish County, Washington Sheriff's Office Inmates
# Task #:       833
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/833
# Dataset Link: https://lokic.locallabs.com/data_sets/787
# Created:      July 2023
#

# ruby mlc.rb --tool="clean::wa::wa_snohomish_inmates"
# ruby mlc.rb --tool="clean::wa::wa_snohomish_inmates" --mode='inmate_name'
# ruby mlc.rb --tool="clean::wa::wa_snohomish_inmates" --mode='court_name'
# ruby mlc.rb --tool="clean::wa::wa_snohomish_inmates" --mode='facility_name'

require 'pry'

Cleaner = MiniLokiC::Formatize::Cleaner

def execute(options = {})
  route = C::Mysql.on(DB01, 'crime_inmate')
  table_description = {
    inmate_name: {
      raw_table:      'wa_snohomish_inmates',
      clean_table:    'wa_snohomish_inmates_names_clean',
      raw_column:     'full_name',
      pattern_column: 'name_pattern',
      clean_column:   'full_name_clean',
    },
    court_name: {
      raw_table:      'wa_snohomish_court_hearings',
      clean_table:    'wa_snohomish_court_hearings_names_clean',
      raw_column:     'court_name',
      clean_column:   'court_name_clean',
    },
    facility_name: {
      raw_table:      'wa_snohomish_holding_facilities',
      clean_table:    'wa_snohomish_holding_facilities_names_clean',
      raw_column:     'facility',
      clean_column:   'facility_clean',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :inmate_name
    recent_date = get_recent_date(table_info, route)
    fill_name_table(table_info, recent_date, where_part, route)
    clean_names_with_pattern(table_info, route)
  when :court_name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_court_names(table_info, route)
  when :facility_name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_court_names(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def clean_court_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  list = route.query(query).to_a
  return if list.empty?

  list.each do |row|
    clean_name = row
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(clean_name[table_info[:raw_column]].dup)
    clean_name[table_info[:clean_column]] = result_name

    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL

    route.query(update_query)
  end
end

def init_name_splitter
  require_relative '../../namesplit/namesplit'
  ns_options = {
    'host' => DB01,
    'database' => 'crime_inmate',
    'table' => 'wa_snohomish_inmates_names_clean',
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

    clean_name[table_info[:clean_column]] = result_name

    last_name   = splitted_name[:last_name]
    middle_name = splitted_name[:middle_name]
    full_name   = splitted_name[:full_name]

    last_name += " #{splitted_name[:last_name2]}" unless splitted_name[:last_name2].to_s.empty?
    suffix_final = splitted_name[:suffix]
    if (last_name.include? "Junior")    || (middle_name.include? "Junior") || (full_name.include? "Junior")
      last_name.gsub!('Junior', '')
      middle_name.gsub!('Junior', '')
      full_name.gsub!('Junior', '')
      full_name = full_name.squish
      full_name.concat(" Jr.")
      suffix_final = "Jr."
    elsif (last_name.include? "Senior") || (middle_name.include? "Senior") || (full_name.include? "Senior")
      last_name.gsub!('Senior', '')
      middle_name.gsub!('Senior', '')
      full_name.gsub!('Senior', '')
      suffix_final = "Sr."
      full_name = full_name.squish
      full_name.concat(" Sr.")
    elsif (last_name.include? "First")  || (middle_name.include? "First")  || (full_name.include? "First")
      last_name.gsub!('First', '')
      middle_name.gsub!('First', '')
      full_name.gsub!('First', '')
      suffix_final = "I"
      full_name = full_name.squish
      full_name.concat(" I")
    elsif (last_name.include? "Second") || (middle_name.include? "Second") || (full_name.include? "Second")
      last_name.gsub!('Second', '')
      middle_name.gsub!('Second', '')
      full_name.gsub!('Second', '')
      suffix_final = "II"
      full_name = full_name.squish
      full_name.concat(" II")
    elsif (last_name.include? "Third")  || (middle_name.include? "Third")  || (full_name.include? "Third")
      last_name.gsub!('Third', '')
      middle_name.gsub!('Third', '')
      full_name.gsub!('Third', '')
      suffix_final = "III"
      full_name = full_name.squish
      full_name.concat(" III")
    elsif (last_name.include? "Fourth") || (middle_name.include? "Fourth") || (full_name.include? "Fourth")
      last_name.gsub!('Fourth', '')
      middle_name.gsub!('Fourth', '')
      full_name.gsub!('Fourth', '')
      suffix_final = "IV"
      full_name = full_name.squish
      full_name.concat(" IV")
    elsif last_name.include? "Fifth"    || (middle_name.include? "Fifth")  || (full_name.include? "Fifth")
      last_name.gsub!('Fifth', '')
      middle_name.gsub!('Fifth', '')
      full_name.gsub!('Fifth', '')
      suffix_final = "V"
      full_name = full_name.squish
      full_name.concat(" V")
    end
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(full_name.squish)}',
          first_name  = '#{escape(splitted_name[:first_name])}',
          middle_name = '#{escape(middle_name.squish)}',
          last_name   = '#{escape(last_name.squish)}',
          suffix      = '#{escape(suffix_final)}'
      WHERE id=#{clean_name['id']}
        AND #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    puts update_query
    route.query(update_query)
  end
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
    GROUP BY r.#{table_info[:raw_column]} order by rand() Limit 2000;
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
           'lfm' AS #{table_info[:pattern_column]},
           MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.id IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]} order by rand() Limit 2000;
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
    text: "*[CLEANING] #580 North Carolina Higher Education Salaries* \n>#{type} #{message}",
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
    # message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
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
         cleaning_dev_name VARCHAR(20) DEFAULT 'Aqeel Anwar',
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

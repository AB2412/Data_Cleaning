# Creator:      Sergii Butrymenko
# Dataset Name: Chicago - Arrests
# Task #:       4
# Migrated:     December 2021

# ruby mlc.rb --tool="clean::il::chicago_police_arrests" --mode='arrestee'

def execute(options = {})
  route = C::Mysql.on(DB01, 'il_raw')
  table_description = {
    arrestee: {
      raw_table: 'chicago_police_arrests',
      clean_table: 'chicago_police_arrests__names_clean',
      raw_column: 'full_name',
      clean_column: 'clean_name',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :arrestee
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_ind_names(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
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
    text: "*[CLEANING #4] Chicago - Arrests* \n>#{type} #{message}",
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
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...")
    constraints = "UNIQUE (#{table_info[:raw_column]})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:state_column]
      state = "#{table_info[:state_column]} VARCHAR(50),"
      usa_adcp_matching_id = "usa_adcp_matching_id INT DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
      usa_adcp_matching_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{type}
         #{state}
         #{usa_adcp_matching_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
      CHARACTER SET utf8 COLLATE utf8_general_ci;
    SQL
    #{local_connection}
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in the source table"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, #{table_info[:type_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?

        insert_query << "('#{escape(item[table_info[:raw_column]])}', '#{item[table_info[:type_column]]}', #{item['scrape_date']}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  str = str.to_s
  return if str == ''
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def clean_ind_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names_to_clean = route.query(query).to_a
  if names_to_clean.empty?
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table."
    return
  end
  names_to_clean.each do |row|
    clean_name = row
    result_name = row[table_info[:raw_column]].strip
    result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name, false)

    puts JSON.pretty_generate(clean_name).yellow

    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated with #{names_to_clean.count} name(s).")
end

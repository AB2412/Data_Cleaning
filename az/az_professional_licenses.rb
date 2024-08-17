# Creator:      Sergii Butrymenko
# Dataset Name: Arizona - Professional Licenses
# Task #:       2
# Migrated:     April 2021

# ruby mlc.rb --tool="clean::az::az_professional_licenses" --mode='names'
# ruby mlc.rb --tool="clean::az::az_professional_licenses" --mode='ind_names'
# ruby mlc.rb --tool="clean::az::az_professional_licenses" --mode='org_names'
# ruby mlc.rb --tool="clean::az::az_professional_licenses" --mode='ind_cities'
# ruby mlc.rb --tool="clean::az::az_professional_licenses" --mode='org_cities'

def execute(options = {})
  table_description = {
      ind_names: {
          src_table:    'arizona_professional_licenseing',
          src_column:   'full_name',
          cln_table:    'arizona_professional_licenseing__individuals_clean',
          raw_column:   'name',
          cln_column:   'name_cleaned',
          sdt_column:   'DATE(created_at)',
      },
      org_names: {
          src_table:    'arizona_professional_licenseing_business',
          src_column:   'business_name',
          cln_table:    'arizona_professional_licenseing__business_clean',
          raw_column:   'name',
          cln_column:   'name_cleaned',
          sdt_column:   'DATE(created_at)',
      },
      ind_cities: {
          src_table: 'arizona_professional_licenseing',
      },
      org_cities: {
          src_table: 'arizona_professional_licenseing_business',
      },
  }

  db01 = C::Mysql.on(DB01, 'usa_raw')
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]

  case mode
  when :ind_names
    names_cleaning(table_info, where_part, db01)
  when :org_names
    names_cleaning(table_info, where_part, db01)
    # org_names_cleaning(where_part, db01)
  when :ind_cities
    cities_cleaning(table_info, where_part, db01)
  when :org_cities
    cities_cleaning(table_info, where_part, db01)
  else
    names_cleaning(table_description[:ind_names], where_part, db01)
    names_cleaning(table_description[:org_names], where_part, db01)
    cities_cleaning(table_description[:ind_cities], where_part, db01)
    cities_cleaning(table_description[:org_cities], where_part, db01)
  end
  db01.close
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
    text: "*[CLEANING #2] Arizona - Professional Licenses* \n>#{type} #{message}",
    as_user: true
  )
end

def names_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_names_table(table_info, recent_date, where_part, route)
  case
  when table_info[:cln_table].include?('individuals')
    clean_ind_names(table_info, route)
  when table_info[:cln_table].include?('business')
    clean_org_names(table_info, route)
  else
    nil
  end
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def clean_ind_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:cln_table]}
    WHERE #{table_info[:cln_column]} IS NULL;
  SQL

  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack("There is no any new names in *#{table_info[:cln_table]}* table.", :info)
    return
  end
  names_list.each do |row|
    clean_name = row
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    parts = row[table_info[:raw_column]].gsub(',', '').rpartition(' ')
    if %w(DPM L.AC. Ph.D. Psy.D).include?(parts.last)
      clean_name[table_info[:cln_column]] = (MiniLokiC::Formatize::Cleaner.person_clean(parts.first, false) + ' ' + parts.last).squeeze(' ').strip
    else
      clean_name[table_info[:cln_column]] = (MiniLokiC::Formatize::Cleaner.person_clean(parts.first + ' ' + parts.last, false).squeeze(' ')).strip
    end
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:cln_table]}
      SET #{table_info[:cln_column]}='#{escape(clean_name[table_info[:cln_column]])}'
      WHERE #{table_info[:cln_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query.red
    route.query(update_query)
  end
end

def clean_org_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:cln_table]}
    WHERE #{table_info[:cln_column]} IS NULL;
  SQL

  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack("There is no any new names in *#{table_info[:cln_table]}* table.", :info)
    return
  end
  names_list.each do |row|
    clean_name = row
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    clean_name[table_info[:cln_column]] = MiniLokiC::Formatize::Cleaner_special.arizona_professional_licenseing_business(clean_name[table_info[:raw_column]])
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:cln_table]}
      SET #{table_info[:cln_column]}='#{escape(clean_name[table_info[:cln_column]])}'
      WHERE #{table_info[:cln_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query.red
    route.query(update_query)
  end
end

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:cln_table]};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table*#{table_info[:cln_table]}*  doesn't exist. Creating it now...", :warning)
    create_table = <<~SQL
      CREATE TABLE #{table}
        (#{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:cln_column]} VARCHAR(255),
         fixed_manually INT(1) DEFAULT 0 NOT NULL,
         skip_it        INT(1) DEFAULT 0 NOT NULL,
         scrape_date    DATE,
         UNIQUE (#{table_info[:raw_column]}));
    SQL
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_names_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT src.#{table_info[:src_column]} AS #{table_info[:raw_column]}, MIN(DATE(created_at)) AS scrape_date
    FROM #{table_info[:src_table]} src
      LEFT JOIN #{table_info[:cln_table]} cln on src.#{table_info[:src_column]} = cln.#{table_info[:raw_column]}
    WHERE cln.#{table_info[:raw_column]} IS NULL
      AND src.#{table_info[:src_column]} IS NOT NULL
      #{"AND DATE(created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY src.#{table_info[:src_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack("No new names found in #{table_info[:src_table]} tables", :info)
  else
    insert_query = <<~SQL
      INSERT IGNORE INTO #{table_info[:cln_table]} (#{table_info[:raw_column]}, scrape_date)
      VALUES
    SQL
    names_list.each do |item|
      next if item[table_info[:raw_column]].nil?
      insert_query << "('#{escape(item[table_info[:raw_column]])}','#{item['scrape_date']}'),"
    end
    insert_query = insert_query.chop + ';'
    puts insert_query.red
    route.query(insert_query)
  end
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  str = str.to_s
  return if str == ''
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

# def insert(db, tab, h, ignore = false, log=false)
#   query = <<~SQL
#     INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')})
#     VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});
#   SQL
#   p query if log
#   db.query(query)
# end


def get_cities_to_clean(table_info, where_part, route)
  query = <<~SQL
    SELECT DISTINCT city, SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(city),',',1), ' AZ', 1) AS trim_city
    FROM #{table_info[:src_table]}
    WHERE city_clean IS NULL
      AND (state like 'AZ%' OR state like 'Arizona%')
      AND city IS NOT NULL AND city<>''
      #{"AND #{where_part}" if where_part}
    ORDER BY trim_city;
  SQL
  puts query.green
  route.query(query).to_a
end

def update_cities(table_info, city_data, where_part, route)
  query = <<~SQL
    UPDATE #{table_info[:src_table]}
    SET city_clean = '#{city_data['city_clean']}'
    WHERE city = '#{city_data['city']}'
      AND city_clean IS NULL
      #{"AND #{where_part}" if where_part}
      AND (state like 'AZ%' OR state like 'Arizona%');
  SQL
  puts query.red
  route.query(query).to_a
end

def cities_cleaning(table_info, where_part, route)
  cities_to_clean = get_cities_to_clean(table_info, where_part, route)
  if cities_to_clean.empty?
    message_to_slack("There is no any new cities in the source table. Exiting...", :info)
    return
  end

  cities_to_clean.each do |row|
    clean_name = row
    clean_name['city_clean'] = MiniLokiC::DataMatching::NearestWord.correct_city_name(row['trim_city'], 'Arizona', 1)

    next if clean_name['city_clean'].nil? || clean_name['city_clean'].empty?

    puts "#{clean_name['trim_city'].rjust(30, ' ')} -- #{clean_name['city_clean']}".yellow
    update_cities(table_info, clean_name, where_part, route)
  end
end

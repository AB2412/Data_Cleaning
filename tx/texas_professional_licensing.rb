# Creator:      Daniel Moskalchuk
# Migrated by:  Sergii Butrymenko
# Dataset Name: Texas Professional Licensing
# Task #:       54
# Created:      November 2021
# Refactored:   November 2022
# Updated:      May 2023

# ruby mlc.rb --tool="clean::tx::texas_professional_licensing" --mode='business_name'
# ruby mlc.rb --tool="clean::tx::texas_professional_licensing" --mode='city_state_zip'
# ruby mlc.rb --tool="clean::tx::texas_professional_licensing" --mode='address'
# ruby mlc.rb --tool="clean::tx::texas_professional_licensing" --mode='license_type'
# ruby mlc.rb --tool="clean::tx::texas_professional_licensing"

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    business_name: {
      raw_table: 'texas_professional_licenseing',
      clean_table: 'texas_professional_licenseing__business_name_clean',
      raw_scrape_date: 'updated_at',
      raw_column: 'business_name',
      clean_column: 'business_name_clean',
      type_column: 'business_name_type',
    },
    city_state_zip: {
      raw_table: 'texas_professional_licenseing',
      clean_table: 'texas_professional_licenseing__city_state_zip_clean',
      raw_scrape_date: 'updated_at',
      raw_column: 'business_citystatezip',
      clean_city: 'city',
      clean_state: 'state',
      clean_zip: 'zip',
      # local_connection: true
    },
    address: {
      raw_table: 'texas_professional_licenseing',
      clean_table: 'texas_professional_licenseing__address_clean',
      raw_scrape_date: 'updated_at',
      raw_column1: 'business_address_line1',
      raw_column2: 'business_address_line2',
      clean_column: 'business_address_clean',
    },
    license_type: {
      raw_table: 'texas_professional_licenseing',
      clean_table: 'texas_professional_licenseing_l_t',
      raw_column: 'license_type',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :business_name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_mixed_names(table_info, route)
  when :city_state_zip
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_city_state_zip(table_info, route)
  when :address
    recent_date = get_recent_date(table_info, route)
    fill_address_table(table_info, recent_date, where_part, route)
    clean_address(table_info, route)
  when :license_type
    check_license_type(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
    # crime_types_cleaning(table_description[:description], where_part, route)
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def fix_unicode_symbols(str)
  replacement_hash = { "\u00a0" => ' ',
                       '’' => "'",
                       '‘' => "'",
                       '“' => '"',
                       '”' => '"',
                       '—' => '-',
                       '–' => '-',

  }
  s = str.dup
  replacement_hash.each { |k, v| s.gsub!(k, v) }
  s
end

def replace_unicode_symbols(str)
  replacement_hash = { "\u00a0" => ' ',
                       '’' => "'",
                       '‘' => "'",
                       '“' => '"',
                       '”' => '"',
                       '—' => '-',
                       '–' => '-',

  }
  s = str.dup
  replacement_hash.each { |k, v| s.gsub!(k, v) }
  s
end

def message_to_slack(message, type = '', to = :owner)
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
  channels = ['UKLB1JGDN']
  channels << 'U02A6JBK9P1' if to == :all
  channels.each do |channel|
    Slack::Web::Client.new.chat_postMessage(
      channel: channel,
      text: "*[CLEANING #54] Texas Professional Licensing* \n>#{type} #{message}",
      as_user: true
    )
  end
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
    raw_columns = "#{table_info[:raw_column]} VARCHAR(255) NOT NULL,"
    if table_info[:clean_column]
      clean_column = "#{table_info[:clean_column]} VARCHAR(255),"
        city_state_zip = nil
      if table_info[:raw_column2]
        raw_columns = "#{table_info[:raw_column1]} VARCHAR(255) NOT NULL,\n#{table_info[:raw_column2]} VARCHAR(255) NOT NULL,"
        constraints = "UNIQUE (#{table_info[:raw_column1]}, #{table_info[:raw_column2]})"
      end
    elsif table_info[:clean_city]
      clean_column = nil
      city_state_zip = <<~HEREDOC
        #{table_info[:clean_city]} VARCHAR(255),
        #{table_info[:clean_state]} VARCHAR(2),
        #{table_info[:clean_zip]} VARCHAR(5),
        city_org_id BIGINT(20) DEFAULT NULL,
      HEREDOC
    else
      clean_column = nil
      city_state_zip = nil
    end
         #{table_info[:clean_column]} VARCHAR(255),
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{raw_columns}
         #{clean_column}
         #{type}
         #{city_state_zip}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    puts create_table.red
    route.query(create_table)
    message_to_slack("Table *#{table_info[:clean_table]}* created successfully")
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}".cyan
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, DATE(MIN(r.#{table_info[:raw_scrape_date]})) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND r.#{table_info[:raw_scrape_date]} >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty? || names_list.first.nil? || (names_list.count == 1 && names_list.first[table_info[:raw_column]].nil?)
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source tables"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?

        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}', #{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      # puts insert_query.red
      route.query(insert_query)
    end
  end
end

def fill_address_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column1]}, r.#{table_info[:raw_column2]}, DATE(MIN(r.#{table_info[:raw_scrape_date]})) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl
        ON r.#{table_info[:raw_column1]} = cl.#{table_info[:raw_column1]}
          AND r.#{table_info[:raw_column2]} = cl.#{table_info[:raw_column2]}
    WHERE cl.#{table_info[:raw_column1]} IS NULL
      #{"AND r.#{table_info[:raw_scrape_date]} >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column1]}, r.#{table_info[:raw_column2]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty? || names_list.first.nil? || (names_list.count == 1 && names_list.first[table_info[:raw_column]].nil?)
    message_to_slack "No new records for *#{table_info[:raw_column1]}, #{table_info[:raw_column2]}* column found in source tables"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column1]}, #{table_info[:raw_column2]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        # next if item[table_info[:raw_column]].nil?

        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column1]])}', '#{escape(item[table_info[:raw_column2]])}', #{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_mixed_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL
    LIMIT 10000;
  SQL
  puts query.green
  cleaned = false
  det = MiniLokiC::Formatize::Determiner.new

  until cleaned
    names_to_clean = route.query(query).to_a
    if names_to_clean.empty?
      message_to_slack "There is no any new _#{table_info[:raw_column]}_ in *#{table_info[:clean_table]}* table."
      cleaned = true
    else
      names_to_clean.each do |row|
        clean_name = row

        result_name = row[table_info[:raw_column]].dup.sub(/^[!\s-]+/, '').sub(/[!\s-]+$/, '')
        while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
          result_name = result_name[1..-1]
        end

        clean_name[table_info[:type_column]] =
          if result_name.include?(',')
            det.determine(result_name)
          else
            'Organization'
          end
        clean_name[table_info[:clean_column]] =
          if clean_name[table_info[:type_column]] == 'Person'
            MiniLokiC::Formatize::Cleaner.person_clean(result_name)
          else
            MiniLokiC::Formatize::Cleaner.org_clean(result_name)
          end

        # puts result_name.green
        # puts JSON.pretty_generate(clean_name).yellow
        update_query = <<~SQL
          UPDATE #{table_info[:clean_table]}
          SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}', #{table_info[:type_column]}='#{clean_name[table_info[:type_column]]}'
          WHERE id=#{clean_name['id']};
        SQL
        # puts update_query.red
        route.query(update_query)
      end
    end
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

def clean_city_state_zip(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_city]} IS NULL
      OR #{table_info[:clean_state]} IS NULL
      OR #{table_info[:clean_zip]} IS NULL;
  SQL
  # puts query
  location_list = route.query(query).to_a
  return nil if location_list.empty?

  location_list.each do |item|
    # puts JSON.pretty_generate(item).green
    # p item[:raw_column]

    location_data = item.dup
    parts = location_data[table_info[:raw_column]].strip.rpartition(' ')
    zip = parts.last
    if zip.match?(/^-/)
      parts = parts.first.strip.rpartition(' ')
      zip = parts.last + zip
    end
    parts = parts.first.strip.rpartition(' ')
    city = parts.first.strip
    state = parts.last.strip

    unless zip.scan(/[^0-9-]/).empty?
      city = "#{city} #{state}"
      state = zip
      zip = ''
    end

    if state.size != 2 || !state.scan(/[^a-z]/i).empty?
      city = "#{city} #{state}"
      state = ''
    end

    city = city.split(/\b/).map(&:capitalize).join.sub(/'S\b/, "'s")
    correct_city = MiniLokiC::DataMatching::NearestWord.correct_city_name(city, state, 1) if city.length > 5
    if correct_city.nil?
      city = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city.dup)).sub(/\bSPG\b/i, 'Springs')
    else
      city = correct_city
    end
    # puts city.black.on_red

    city, city_org_id = get_city_org_id(state, city, route)
    # puts "#{city_data[table_info[:raw_column]]} -- #{city_data[table_info[:clean_column]]} -- #{usa_adcp_matching_id}".yellow
    query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_city]} = '#{escape(city)}',
        #{table_info[:clean_state]} = '#{escape(state)}',
        #{table_info[:clean_zip]} = '#{escape(zip[0..4])}',
        city_org_id = #{city_org_id.nil? ? "NULL" : "#{city_org_id}"}
      WHERE id = #{item['id']};
    SQL
    # puts query.red
    route.query(query)
  end
end

def get_city_org_id(state_code, city, route)
  query = <<~SQL
    SELECT short_name, pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                    FROM hle_resources_readonly_sync.usa_administrative_division_states
                    WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    [city, 'NULL']
  else
    [res.first['short_name'], res.first['pl_production_org_id']]
  end
end

def clean_address(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column1]}, #{table_info[:raw_column2]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  # puts query
  address_list = route.query(query).to_a
  return nil if address_list.empty?

  address_list.each do |item|
    address = [item[table_info[:raw_column1]], item[table_info[:raw_column2]]]
    address.reverse! if address.first.start_with?(/#|ss?te\b|suit|unite?\b|apt\b|bld|fl(at)?\b|No|lot\b/i)
    address = MiniLokiC::Formatize::Address.abbreviated_streets(address.join(' '))
    # puts "#{city_data[table_info[:raw_column]]} -- #{city_data[table_info[:clean_column]]} -- #{usa_adcp_matching_id}".yellow
    query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(address)}'
      WHERE id = #{item['id']}
        AND #{table_info[:clean_column]} IS NULL;
    SQL
    # puts query.red
    route.query(query)
  end
end

def check_license_type(table_info, route)
  query = <<~SQL
    SELECT DISTINCT #{table_info[:raw_column]}
    FROM #{table_info[:raw_table]}
    WHERE #{table_info[:raw_column]} NOT IN (SELECT #{table_info[:raw_column]} FROM #{table_info[:clean_table]});
  SQL
  license_type = route.query(query).to_a
  if license_type.empty?
    message_to_slack("There is no any new license type in *#{table_info[:clean_table]}* table.", :info)
  else
    insert_query = <<~SQL.chop
      INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]})
        VALUES #{license_type.map {|i| "(#{i[table_info[:raw_column]]}),"}.join}
    SQL
    # p "#{insert_query.chop};"
    route.query("#{insert_query.chop};")
    message_to_slack("#{license_type.size} license type(s) in *db01.usa_raw.#{table_info[:clean_table]}* should be cleaned manually.", :warning, :all)
  end
end

# Creator:      Sergii Butrymenko
# Dataset Name: Arizona State CC Data Cleaning
# Task #:       64
# Migrated:     March 2022

# ruby mlc.rb --tool="clean::az::az_campaign_finance"
# ruby mlc.rb --tool="clean::az::az_campaign_finance" --mode='ctc_candidate'
# ruby mlc.rb --tool="clean::az::az_campaign_finance" --mode='ctc_committee'
# ruby mlc.rb --tool="clean::az::az_campaign_finance" --mode='ctc_city'
#
# ruby mlc.rb --tool="clean::az::az_campaign_finance" --mode='cand_contributor'
# ruby mlc.rb --tool="clean::az::az_campaign_finance" --mode='cand_recipient'
# ruby mlc.rb --tool="clean::az::az_campaign_finance" --mode='cmte_contributor'
# ruby mlc.rb --tool="clean::az::az_campaign_finance" --mode='cmte_recipient'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    ctc_candidate: {
      raw_table: 'arizona_candidate_to_committe_fixed',
      clean_table: 'arizona_candidates__clean',
      raw_column: 'filer_name',
      clean_column: 'filer_name_clean',
    },
    ctc_committee: {
      raw_table: 'arizona_candidate_to_committe_fixed',
      clean_table: 'arizona_committees__clean',
      raw_column: 'candidate_committee',
      clean_column: 'candidate_committee_clean',
    },
    ctc_city: {
      raw_table: 'arizona_candidate_to_committe_fixed',
      clean_table: 'arizona_candidates__cities_clean',
      raw_city: 'city',
      clean_city: 'city_clean',
      raw_state: 'state',
      clean_state: 'state',
    },
    cand_contributor: {
      raw_table: 'arizona_candidate_contributions_fixed',
      clean_table: 'arizona_candidate__contributors_clean',
      raw_column: 'contributor_complete_name',
      clean_column: 'contributor_complete_name_clean',
      type_column: 'name_type',
    },
    cand_recipient: {
      raw_table: 'arizona_candidate_expenditures_fixed',
      clean_table: 'arizona_candidate__recipients_clean',
      raw_column: 'recipient_name',
      clean_column: 'recipient_name_clean',
      type_column: 'name_type',
    },
    cmte_contributor: {
      raw_table: 'arizona_committee_contributions_fixed',
      clean_table: 'arizona_committee__contributors_clean',
      raw_column: 'contributor_complete_name',
      clean_column: 'contributor_complete_name_clean',
      type_column: 'name_type',
    },
    cmte_recipient: {
      raw_table: 'arizona_committee_expenditures_fixed',
      clean_table: 'arizona_committee__recipients_clean',
      raw_column: 'recipient_name',
      clean_column: 'recipient_name_clean',
      type_column: 'name_type',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :ctc_candidate
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_ind_names(table_info, route)
  when :ctc_committee
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_cmte_names(table_info, route)
  when :ctc_city
    recent_date = get_recent_date(table_info, route)
    fill_city_table(table_info, recent_date, where_part, route)
    clean_cities(table_info, route)
  when :cand_contributor, :cmte_contributor, :cand_recipient, :cmte_recipient
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_mixed_names(table_info, route)
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
    text: "*[CLEANING #64] Arizona State CC Data Cleaning* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  return str if str.nil?

  str = str.to_s
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

# Committee Cleaning

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
    # local_connection = if table_info[:local_connection]
    #                      <<~SQL
    #                       employer_category VARCHAR(255),
    #                       emp_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
    #                       state VARCHAR(50) DEFAULT 'New Mexico',
    #                      SQL
    #                    else
    #                      nil
    #                    end
    raw_column = table_info.key?(:raw_city) ? table_info[:raw_city] : table_info[:raw_column]
    clean_column = table_info.key?(:clean_city) ? table_info[:clean_city] : table_info[:clean_column]
    constraints = "UNIQUE (#{raw_column})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:clean_state]
      state = "#{table_info[:clean_state]} VARCHAR(2),"
      usa_adcp_matching_id = "usa_adcp_matching_id INT DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:clean_state]}, #{table_info[:raw_city]})"
    else
      state = nil
      usa_adcp_matching_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{raw_column} VARCHAR(255) NOT NULL,
         #{clean_column} VARCHAR(255) DEFAULT NULL,
         #{type}
         #{state}
         #{usa_adcp_matching_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
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

def clean_cmte_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names_list = route.query(query).to_a

  if names_list.empty?
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table.", :info
    return
  end
  names_list.each do |row|
    # puts row
    clean_name = row
    # puts clean_name[table_info[:raw_column]].to_s.cyan
    result_name = row[table_info[:raw_column]].dup.sub(/\bCTE\b/, 'Committee to Elect').gsub(/[“”]/, '"').gsub('’', "'").strip
    while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
      result_name = result_name[1..-1]
    end

    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/ C\/O.+/i, '').sub('&amp;', '&').sub('&#39;', "'").sub(/\bCOM\b/i, 'Committee').sub(/\bST REP\b/i, 'State Representative').sub(/\bSTATE REP\b/i, 'State Representative').sub(/\bDEM\b/i, 'Democratic').sub(/\bREP\b/i, 'Republican').sub(/\bCO\b/i, 'County').sub(/\bEMPL\b/i, 'Employee').sub(/\bEMPLS\b/i, 'Employees').sub(/\bGOV\b/i, 'Government').sub(/\bLEG\b/i, 'Legislative').sub(/\bCAMP\b/i, 'Campaign').sub(/\bTWP\b/i, 'Township').sub(/\bPOL\b/i, 'Political').sub(/\bFED\b/i, 'Federation').sub(/\bWRKRS\b/i, 'Workers').sub(/\bAz\b/i, 'Arizona').strip.squeeze(' '))
    # puts JSON.pretty_generate(result_name).yellow
    # insert_query = <<~SQL
    #     INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column_ct]}, #{table_info[:clean_column]}, scrape_date)
    #     VALUES ('#{escape(clean_name[table_info[:raw_column_rt]])}', '#{escape(result_name)}', '#{row['scrape_date']}');
    # SQL
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}'
      WHERE (#{table_info[:clean_column]} IS NULL OR #{table_info[:clean_column]} = '')
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query.red
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.", :info)
end

# Candidate Cleaning

def fill_table(table_info, recent_date, where_part, route)
    # SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(last_scrape_date) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND last_scrape_date >= '#{recent_date}'" if recent_date && !where_part}
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
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?

        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}',#{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      # puts insert_query.red
      route.query(insert_query)
    end
  end
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
    message_to_slack "There is no any new name in #{table_info[:clean_table]} table.", :info
    return
  end
  names_to_clean.each do |row|
    clean_name = row
    result_name = MiniLokiC::Formatize::Cleaner.person_clean(row[table_info[:raw_column]].dup.gsub(/[“”]/, '"').strip)
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

# Contributor Cleaning

def clean_mixed_names(table_info, route)
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
  det = MiniLokiC::Formatize::Determiner.new
  names_to_clean.each do |row|
    clean_name = row
    # puts clean_name[table_info[:raw_column]].to_s.cyan
    result_name = row[table_info[:raw_column]].dup.strip.sub(/^(mr\. &amp; mrs\. |mr?[r|s]\.? )/i, '').sub(/\s+\./, '.').sub(/\.{2,}/, '.').sub(/,{2,}/, ',').gsub('&amp;', '&')
    while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
      result_name = result_name[1..-1]
    end

    clean_name[table_info[:type_column]] = det.determine(result_name)

    result_name = clean_name[table_info[:type_column]] == 'Person' ? MiniLokiC::Formatize::Cleaner.person_clean(result_name) : MiniLokiC::Formatize::Cleaner.org_clean(result_name)

    if result_name.match?(/(FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|PEOPLE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)$/i)
      m = result_name.match(/(.+) (FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|PEOPLE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)/i).captures
      clean_name[table_info[:clean_column]] =
        if m[0].match?(/,/)
          m[1] + ' ' + m[0].match(/(.+), ?(.*)/).captures.reverse.join(' ')
        else
          m.join(' ')
        end
    else
      clean_name[table_info[:clean_column]] = result_name
    end

    # clean_name['skip_it'] = 1 unless clean_name[table_info[:raw_column]].match?(/[a-z]/i)
    # puts result_name.green
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}', #{table_info[:type_column]}='#{clean_name[table_info[:type_column]]}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    # puts update_query.red
    route.query(update_query)
  end
  message_to_slack("Table *#{table_info[:clean_table]}* was updated.")
end

def fill_city_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_state]}, r.#{table_info[:raw_city]}, MIN(last_scrape_date) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl
        ON r.#{table_info[:raw_state]} = cl.#{table_info[:clean_state]}
        AND r.#{table_info[:raw_city]} = cl.#{table_info[:raw_city]}
    WHERE cl.#{table_info[:clean_city]} IS NULL
      #{"AND last_scrape_date >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_state]}, r.#{table_info[:raw_city]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack("No new records for *#{table_info[:raw_city]}* column found in source tables", :info)
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:clean_state]}, #{table_info[:raw_city]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        insert_query << "('#{escape(item[table_info[:raw_state]])}','#{escape(item[table_info[:raw_city]])}','#{item[:scrape_date]}'),"
      end
      insert_query = "#{insert_query.chop};"
      # puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_cities(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_city]}, #{table_info[:clean_state]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_city]} IS NULL;
  SQL
  puts query
  city_list = route.query(query).to_a
  if city_list.empty?
    message_to_slack "There is no any new cities in *#{table_info[:clean_table]}* table.", :info
    return
  end
  city_list.each do |item|
    # puts JSON.pretty_generate(item).cyan

    city_name = item[table_info[:raw_city]].dup.split(/\b/)
                  .map(&:capitalize).join.sub(/'S\b/,"'s").squeeze(' ')
                  .sub(/\bhts.?\b/i, 'Heights')
                  .sub(/\btwn?s?p.?\b/i, 'Township')
                  .sub(/\bjct.?\b/i, 'Junction')
                  .sub(/\bSPG\b/i, 'Springs')
    # puts city_name
    cn = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name.dup, item[table_info[:clean_state]], 1) if city_name.length > 5
    city_name = cn if cn

    city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name))
    usa_adcp_matching_id = get_usa_adcp_matching_id(item[table_info[:clean_state]], city_name, route)
    # puts "#{item[table_info[:raw_city]]} -- #{city_name} -- #{usa_adcp_matching_id}".yellow
    # puts JSON.pretty_generate(city_name).yellow

    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_city]}='#{escape(city_name)}',
          usa_adcp_matching_id = #{usa_adcp_matching_id.nil? ? "NULL" : "#{usa_adcp_matching_id}"}
      WHERE #{table_info[:clean_city]} IS NULL
        AND #{table_info[:raw_city]}='#{escape(item[table_info[:raw_city]])}'
        AND #{table_info[:clean_state]}='#{escape(item[table_info[:clean_state]])}';
    SQL
    # puts update_query.red
    route.query(update_query)
  end
end

def get_usa_adcp_matching_id(state_code, city, route)
  query = <<~SQL
    SELECT id, pl_production_org_id
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
    nil
  else
    res.first['id']
  end
end

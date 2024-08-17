# Creator:      Sergii Butrymenko
# Dataset Name: US Schools: Alabama education
# Task #:       103
# Migrated:     April 2023

# ruby mlc.rb --tool="clean::al::alabama_education" --mode='schools'
# ruby mlc.rb --tool="clean::al::alabama_education" --mode='check_unmatched'

def execute(options = {})
  route = C::Mysql.on(DB01, 'us_schools_raw')
  mode = options['mode']&.to_sym
  case mode
  when :schools
    clean_school_names(route)
  when :check_unmatched
    check_unmatched(route)
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
    text: "*[CLEANING #103] US Schools: Alabama education* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  return str if str.nil?

  str = str.to_s
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def escape_or_null(str)
  return 'NULL' if str.nil?

  "'#{str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")}'"
end

# Schools Cleaning

def clean_school_names(route)
  query = <<~SQL
    SELECT id, name
    FROM al_general_info
    WHERE deleted=0
      AND is_district=0
      AND name_clean IS NULL;
  SQL
  puts query.green
  school_names = route.query(query, symbolize_keys: true).to_a

  school_names.each do |item|
    puts JSON.pretty_generate(item).yellow
    # clean_school_name = item[:org_name].dup.sub(/\bRHS\b/i, 'Regional High School')
    #                       .sub(/\bMid\b/i, 'Middle')
    #                       .sub(/\bEd\b/i, 'Education')
    #                       .gsub(/\b(Jr|Sr)\b\.?/i){|i| i[0].downcase == 'j' ? 'Junior' : 'Senior'}
    #                       .gsub(/(?<!\bon)(?:\s)\b([a-z]|Capt|Col|Dr|lt|Mt|Wm|Gr|St)(?=\s)(?!\.)/i) {|i| "#{i.capitalize}."}
    # clean_school_name = "#{clean_school_name} School" unless clean_school_name.match?(/\b(school|preschool|center|academy|conservatory|hospital|ELC|College|program)\b/i)
    clean_school_name = item[:name].gsub(/\bsch\b/i, 'School').gsub(/(?<!')\b([a-z]|Capt|Col|Dr|lt|Mt|Wm|Gr|St)(?=\s)(?!\.)/i) {|i| "#{i.capitalize}."}

    puts clean_school_name
    puts "#{item[:name]} >>> #{clean_school_name}".cyan if item[:name] != clean_school_name
    insert_query = <<~SQL
      UPDATE al_general_info
      SET name_clean = '#{escape(clean_school_name)}'
      WHERE id = #{item[:id]}
        AND name='#{escape(item[:name])}'
        AND name_clean IS NULL;
    SQL

    puts insert_query
    route.query(insert_query)
  end
end

def check_unmatched(route)
  query = <<~SQL
    SELECT COUNT(*) AS total_unmatched,
           SUM(IF(city IS NULL, 0, 1)) AS with_city,
           SUM(IF(city IS NULL, 1, 0)) AS without_city
    FROM us_schools_raw.al_general_info
    WHERE id IN
      (
        SELECT DISTINCT general_id FROM us_schools_raw.al_accountability_indicators
        UNION
        SELECT DISTINCT general_id FROM us_schools_raw.al_college_career_readiness
        UNION
        SELECT DISTINCT general_id FROM us_schools_raw.al_enrollment
        UNION
        SELECT DISTINCT general_id FROM us_schools_raw.al_schools_assessment
      )
      AND pl_production_org_id IS NULL;
  SQL
  counts = route.query(query, symbolize_keys: true).to_a.first
  unless counts[:total_unmatched].zero?
    message_to_slack("#{counts[:total_unmatched]} unmatched orgs found in *db01.us_schools_raw.al_general_info* table and #{counts[:with_city]} of them have address (city).", counts[:with_city].zero? ? :info : :warning)
  end
end
